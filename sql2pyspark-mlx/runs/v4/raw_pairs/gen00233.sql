WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS attr_key, value
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        value_2
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) t AS value_2
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(value_2) AS value_2_length,
        COALESCE(value_2, 'DIRECT') AS value_2_filled
    FROM flattened
    WHERE value_2 IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN value_2_length >= 78 THEN 'LARGE' ELSE 'SMALL' END AS value_2_length_tier,
    customer_id,
    SORT_ARRAY(COLLECT_SET(value_2)) AS value_2_list,
    SORT_ARRAY(COLLECT_SET(value_2)) AS value_2_list_2,
    ROUND(SUM(value_2_length) / NULLIF(COUNT(CASE WHEN value_2 = 'WELCOME5' THEN 1 END), 0), 2) AS value_2_length_per_welcome5,
    ROUND(SUM(value_2_length) / NULLIF(COUNT(CASE WHEN value_2 = 'VIP' THEN 1 END), 0), 2) AS value_2_length_per_vip
FROM valid
GROUP BY CASE WHEN value_2_length >= 78 THEN 'LARGE' ELSE 'SMALL' END, customer_id;
