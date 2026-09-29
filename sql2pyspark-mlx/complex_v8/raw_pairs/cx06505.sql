WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        ABS(premium - 3465.4) AS premium_distance
    FROM policies
    WHERE (start_date < DATE '2025-05-30' OR holder_id IS NOT NULL)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        premium_distance,
        MAX(premium_distance) OVER (PARTITION BY product, holder_id) AS product_holder_id_max_premium_distance,
        ROW_NUMBER() OVER w_start_date_month_holder_id_b AS seq_no,
        ROUND(premium_distance / NULLIF(SUM(premium_distance) OVER (PARTITION BY product, holder_id), 0), 4) AS premium_distance_share_of_product_holder_id,
        LEAD(start_date) OVER (PARTITION BY MONTH(start_date), holder_id ORDER BY start_date DESC NULLS FIRST, id) AS next_start_date,
        MIN(premium_distance) OVER (PARTITION BY product, holder_id) AS product_holder_id_min_premium_distance
    FROM cleaned
    WINDOW w_start_date_month_holder_id_b AS (PARTITION BY MONTH(start_date), holder_id ORDER BY start_date DESC NULLS FIRST, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        status,
        seq_no,
        YEAR(next_start_date) AS next_start_date_year,
        ROUND(100 * COUNT(CASE WHEN product IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_product_is_not_null,
        COUNT(DISTINCT product) AS distinct_products,
        DATEDIFF(MAX(next_start_date), MIN(next_start_date)) AS next_start_date_span_days,
        ROUND(AVG(premium_distance_share_of_product_holder_id), 2) AS avg_premium_distance_share_of_product_holder_id
    FROM metrics
    GROUP BY status, seq_no, YEAR(next_start_date)
)
-- Native SQL UNPIVOT (wide to long)
SELECT status, seq_no, metric, reading
FROM (
    SELECT status, seq_no, avg_premium_distance_share_of_product_holder_id, pct_product_is_not_null
    FROM grouped
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (avg_premium_distance_share_of_product_holder_id, pct_product_is_not_null)
) unp
