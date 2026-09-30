WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        units,
        ADD_MONTHS(sale_date, 12) AS sale_date_plus_months
    FROM store_sales
    WHERE sale_date > DATE '2023-09-09'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY units DESC) AS units_percent_rank,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM base
    WINDOW w_part AS (ORDER BY units, id)
),
scored_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER w_part AS units_bucket,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY units DESC, id) AS units_bucket_2,
        LAG(units_percent_rank, 2, 0) OVER w_part AS prev_units_percent_rank,
        RANK() OVER (PARTITION BY product_id ORDER BY seq_no NULLS FIRST) AS seq_no_rank,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY seq_no, id) AS seq_no_bucket
    FROM scored
    WINDOW w_part AS (ORDER BY units, id)
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.*,
        d.stock_qty,
        d.brand
    FROM scored_store_sales e
    INNER JOIN products d
        ON e.product_id = d.id
)
-- Multi-level aggregation
SELECT
    brand,
    CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
    CASE WHEN units_bucket_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END AS units_bucket_2_tier,
    APPROX_PERCENTILE(units_percent_rank, 0.95) AS units_percent_rank_p95,
    ROUND(AVG(units), 2) AS avg_units,
    COUNT_IF(brand = 'Globex') AS globex_events,
    COUNT_IF(brand = 'Umbrella') AS umbrella_events
FROM enriched
GROUP BY brand, CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN units_bucket_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END
HAVING globex_events >= 1
ORDER BY globex_events DESC
