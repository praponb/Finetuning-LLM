WITH joined AS (
    -- Attach stores attributes
    SELECT s.*, p.region
    FROM store_sales s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM joined
    EXCEPT DISTINCT
    SELECT id
    FROM store_sales
    WHERE sale_date <= '2025-09-23'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.id = k.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        region,
        MAX(revenue) OVER w_hist AS sale_date_year_max_revenue,
        ROUND(units / SUM(units) OVER (PARTITION BY region), 4) AS units_share,
        ROUND(units / SUM(units) OVER (PARTITION BY DATE_TRUNC('WEEK', sale_date), product_id), 4) AS units_share_2,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id, region), 4) AS units_share_3
    FROM kept_rows
    WINDOW w_hist AS (PARTITION BY YEAR(sale_date), store_id)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY units_share_3 DESC) AS units_share_3_percent_rank,
        STDDEV_SAMP(sale_date_year_max_revenue) OVER (PARTITION BY MONTH(sale_date), store_id) AS sale_date_month_stddev_samp_sale_date_year_max_revenue,
        RANK() OVER (ORDER BY sale_date_year_max_revenue DESC) AS sale_date_year_max_revenue_rank
    FROM scored
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(sale_date_month_stddev_samp_sale_date_year_max_revenue / SUM(sale_date_month_stddev_samp_sale_date_year_max_revenue) OVER (PARTITION BY region), 4) AS sale_date_month_stddev_samp_sale_date_year_max_revenue_share,
        RANK() OVER w_hist AS units_share_2_rank,
        MIN(units_share_2) OVER (PARTITION BY store_id) AS store_id_min_units_share_2,
        ROUND(sale_date_year_max_revenue / SUM(sale_date_year_max_revenue) OVER (PARTITION BY DATE_TRUNC('WEEK', sale_date)), 4) AS sale_date_year_max_revenue_share,
        ROUND(sale_date_year_max_revenue_rank / SUM(sale_date_year_max_revenue_rank) OVER (PARTITION BY region, store_id), 4) AS sale_date_year_max_revenue_rank_share
    FROM metrics
    WHERE sale_date_month_stddev_samp_sale_date_year_max_revenue >= 1337
    WINDOW w_hist AS (ORDER BY units_share_2 DESC NULLS LAST)
)
-- Join a dimension and aggregate
SELECT
    dim.city,
    dim.region AS store_region,
    src.region AS store_sale_region,
    COUNT(*) AS rows_total,
    SUM(src.units_share) AS total_units_share,
    MAX(src.store_id_min_units_share_2) AS max_store_id_min_units_share_2
FROM with_windows src
INNER JOIN stores dim ON src.store_id = dim.id
GROUP BY GROUPING SETS ((dim.city, dim.region, src.region), (dim.city), ());
