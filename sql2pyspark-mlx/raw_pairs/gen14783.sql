WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS key, value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(key, 1, 2) AS key_prefix,
        SIZE(coupon_codes) AS coupon_codes_count,
        COALESCE(key, 'OTHER') AS key_clean
    FROM lines
    WHERE key NOT IN ('gift', 'utm_source')
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM cleaned
    INTERSECT ALL
    SELECT id, customer_id
    FROM carts
    WHERE created_at < '2025-07-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN matched_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM verified
    INTERSECT
    SELECT id, customer_id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 12 AND 18
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        key,
        value,
        key_prefix,
        key_clean,
        SUM(coupon_codes_count) OVER (PARTITION BY customer_id) AS customer_id_sum_coupon_codes_count,
        RANK() OVER (PARTITION BY key ORDER BY coupon_codes_count DESC NULLS FIRST) AS coupon_codes_count_rank,
        MAX(coupon_codes_count) OVER (PARTITION BY key, customer_id) AS key_max_coupon_codes_count,
        RANK() OVER (PARTITION BY customer_id ORDER BY coupon_codes_count NULLS FIRST) AS coupon_codes_count_rank_2
    FROM reconciled
)
SELECT
    key,
    value,
    key_prefix,
    key_clean,
    customer_id_sum_coupon_codes_count,
    coupon_codes_count_rank,
    key_max_coupon_codes_count,
    coupon_codes_count_rank_2,
    CASE WHEN key RLIKE '^(utm_source|gift)$' THEN 'GROUP_B' WHEN key RLIKE '^(device|weight_kg)$' THEN 'CORE' ELSE 'GROUP_A' END AS key_zone
FROM with_windows
ORDER BY coupon_codes_count_rank DESC, coupon_codes_count_rank_2 DESC
