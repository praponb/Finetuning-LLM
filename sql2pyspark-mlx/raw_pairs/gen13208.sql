WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS key, attr_value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(key) AS key_length,
        LOWER(key) AS key_lower
    FROM flattened
    WHERE key IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM filtered
    EXCEPT ALL
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(key_length) OVER (PARTITION BY key) AS key_count_key_length,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY key_length DESC NULLS LAST) AS key_length_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY key_length DESC) AS key_length_percent_rank_2
    FROM kept_rows
),
base AS (
    -- Keep recent, valid rows
    SELECT
        key,
        attr_value,
        key_count_key_length,
        key_length_percent_rank_2,
        key_length + key_length_percent_rank_2 AS key_length_plus_key_length_percent_rank_2,
        ROUND(key_length_percent_rank_2 * 1.1, 2) AS key_length_percent_rank_2_adjusted
    FROM enriched
    WHERE key_length IS NOT NULL
),
enriched_carts AS (
    -- Ranking and gap features
    SELECT *, MIN(key_count_key_length) OVER (PARTITION BY key) AS key_min_key_count_key_length
    FROM base
    WHERE key_count_key_length > 8
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(key_length_plus_key_length_percent_rank_2 * 0.9, 2) AS adjusted_key_length_plus_key_length_percent_rank_2
    FROM enriched_carts
    WHERE key_length_plus_key_length_percent_rank_2 <= 70
)
SELECT
    key_count_key_length,
    key_length_plus_key_length_percent_rank_2,
    key_length_percent_rank_2_adjusted,
    key_min_key_count_key_length,
    adjusted_key_length_plus_key_length_percent_rank_2
FROM cleaned;
