WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE product = 'TRAVEL'
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        COUNT(*) FILTER (WHERE product = 'HEALTH') AS policy_product_count
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_product_count, 0) AS policy_product_count
    FROM cleaned s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
)
SELECT id, metric, amount
FROM (
    SELECT id, premium_bucket, policy_product_count
    FROM summarized_rows
) src
UNPIVOT (
    amount FOR metric IN (premium_bucket, policy_product_count)
) unp
ORDER BY id, metric
