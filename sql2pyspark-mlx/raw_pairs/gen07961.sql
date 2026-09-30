WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS k, value
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        value,
        EXPLODE_OUTER(coupon_codes) AS value_2
    FROM exploded
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(value_2) AS value_2_length
    FROM exploded_carts
    WHERE value_2 = 'SPRING10'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY value_2 ORDER BY value_2_length DESC NULLS FIRST) AS value_2_length_dense_rank,
        CUME_DIST() OVER (PARTITION BY value_2 ORDER BY value_2_length DESC) AS value_2_length_cume_dist
    FROM valid
),
valid_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        value_2_length_cume_dist * value_2_length_dense_rank AS value_2_length_cume_dist_times_value_2_length_dense_rank,
        COALESCE(value_2_length, 0) AS value_2_length_filled,
        COALESCE(value_2, 'NONE') AS resolved_value_2
    FROM scored
    WHERE value_2 IS NOT NULL
)
SELECT
    id,
    value,
    value_2,
    value_2_length,
    value_2_length_dense_rank,
    value_2_length_cume_dist,
    value_2_length_cume_dist_times_value_2_length_dense_rank,
    value_2_length_filled,
    CASE WHEN value_2 RLIKE '^(WELCOME5|SPRING10)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS value_2_zone
FROM valid_carts
ORDER BY value_2_zone DESC NULLS LAST, value_2_length_dense_rank
