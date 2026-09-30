WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        CONCAT_WS(': ', product, status) AS product_status_label,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE holder_id <= 4
      AND product NOT IN ('TRAVEL', 'AUTO')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(premium_bucket, 2) OVER (PARTITION BY product, status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_bucket,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY MONTH(end_date), holder_id)) AS distinct_status_in_end_date_month_holder_id,
        ROUND(premium_bucket - AVG(premium_bucket) OVER (PARTITION BY MONTH(end_date), holder_id), 2) AS premium_bucket_vs_end_date_month_holder_id_avg
    FROM recent
)
-- Subtotals with CUBE
SELECT
    status,
    product,
    distinct_status_in_end_date_month_holder_id,
    DATEDIFF(MAX(end_date), MIN(end_date)) AS end_date_span_days,
    SUM(distinct_status_in_end_date_month_holder_id) AS total_distinct_status_in_end_date_month_holder_i
FROM with_trends
GROUP BY CUBE(status, product, distinct_status_in_end_date_month_holder_id)
ORDER BY status NULLS LAST, product NULLS LAST, distinct_status_in_end_date_month_holder_id NULLS LAST
