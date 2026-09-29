WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('WEEK', sale_date) AS sale_date_period,
        GREATEST(units, revenue) AS greatest_units_revenue
    FROM store_sales
    WHERE revenue <= 4000
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(sale_date_period) OVER (PARTITION BY product_id ORDER BY sale_date_period ASC, id ASC) IS NULL
                 OR sale_date_period > LAG(sale_date_period) OVER (PARTITION BY product_id ORDER BY sale_date_period ASC, id ASC) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        store_id,
        product_id,
        units,
        sale_date_period,
        SUM(is_new_session) OVER (PARTITION BY product_id ORDER BY sale_date_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        units,
        island_id,
        AVG(units) OVER (PARTITION BY store_id ORDER BY units ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS units_avg_2_prior,
        LEAD(island_id, 1) OVER (PARTITION BY store_id ORDER BY island_id ASC, id ASC) AS next_island_id,
        STDDEV_SAMP(island_id) OVER w_part AS sale_date_period_month_stddev_samp_island_id
    FROM islands
    WHERE units >= 20
    WINDOW w_part AS (PARTITION BY MONTH(sale_date_period))
),
enriched_store_sales AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.units,
        e.island_id,
        e.units_avg_2_prior,
        e.next_island_id,
        e.sale_date_period_month_stddev_samp_island_id,
        d.city,
        d.opened_date,
        d.region
    FROM enriched e
    LEFT JOIN stores d
        ON e.store_id = d.id
)
-- Multi-level aggregation
SELECT
    island_id,
    region,
    APPROX_PERCENTILE(sale_date_period_month_stddev_samp_island_id, 0.9) AS sale_date_period_month_stddev_samp_island_id_p90,
    COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count,
    COUNT(DISTINCT region) AS distinct_regions,
    COUNT(DISTINCT store_id) AS distinct_store_ids
FROM enriched_store_sales
GROUP BY island_id, region
ORDER BY east_count DESC;
