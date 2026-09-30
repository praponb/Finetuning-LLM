WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(revenue, 0) AS revenue_filled
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-07-18')
      AND sale_date BETWEEN '2023-06-01' AND '2024-06-01'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        sale_date,
        units,
        RANK() OVER (PARTITION BY MONTH(sale_date) ORDER BY revenue) AS revenue_rank,
        AVG(revenue_filled) OVER (ORDER BY sale_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS revenue_filled_avg_3,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('week', sale_date), store_id ORDER BY units DESC, id) AS units_bucket
    FROM base
    WHERE store_id = 3
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(units_bucket) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS units_bucket_avg_2,
        SUM(units) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units,
        LAST_VALUE(units) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY revenue_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units
    FROM enriched
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_id,
        sale_date,
        revenue_rank,
        revenue_filled_avg_3,
        units_bucket_avg_2,
        cumulative_units
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC NULLS LAST, id) AS rank_in_group
        FROM metrics
    ) t
    WHERE rank_in_group = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        revenue_filled_avg_3 * units_bucket_avg_2 AS revenue_filled_avg_3_times_units_bucket_avg_2,
        GREATEST(revenue_filled_avg_3, revenue_rank) AS greatest_revenue_filled_avg_3_revenue_rank
    FROM latest
    WHERE sale_date > TO_DATE('2023-04-22')
      AND store_id IN (1, 4, 5, 6)
),
base_store_sales AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(greatest_revenue_filled_avg_3_revenue_rank) AS greatest_revenue_filled_avg_3_revenue_rank_whole,
        revenue_filled_avg_3_times_units_bucket_avg_2 + greatest_revenue_filled_avg_3_revenue_rank AS revenue_filled_avg_3_times_units_bucket_avg_2_plus_greatest_revenue_filled_avg_3_revenue_rank
    FROM cleaned
    WHERE sale_date > TO_DATE('2023-03-30')
)
-- Join a dimension and aggregate
SELECT
    p.region,
    p.city,
    COUNT(*) AS cnt,
    MIN(a.units_bucket_avg_2) AS min_units_bucket_avg_2,
    SUM(a.cumulative_units) AS total_cumulative_units,
    GROUPING_ID() AS gid
FROM base_store_sales a
INNER JOIN stores p ON a.store_id = p.id
GROUP BY ROLLUP (p.region, p.city)
