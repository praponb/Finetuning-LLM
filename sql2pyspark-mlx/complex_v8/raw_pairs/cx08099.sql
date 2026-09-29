WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        DATEDIFF(DATE '2025-06-30', start_date) AS days_since_start_date
    FROM policies
    WHERE premium > 2289.0
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY status, product ORDER BY premium_bucket, id) AS premium_bucket_quintile,
        SUM(premium_bucket) OVER (PARTITION BY holder_id, status) AS holder_id_status_sum_premium_bucket,
        ROUND(days_since_start_date - AVG(days_since_start_date) OVER (PARTITION BY holder_id, status), 2) AS days_since_start_date_vs_holder_id_status_avg
    FROM prepared_policys
)
SELECT id, metric_name, reading
FROM (
    SELECT id, days_since_start_date_vs_holder_id_status_avg, premium
    FROM with_windows
) src
UNPIVOT (
    reading FOR metric_name IN (days_since_start_date_vs_holder_id_status_avg, premium)
) unp
