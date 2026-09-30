WITH lines AS (
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS value
    FROM carts
),
valid AS (
    SELECT
        *,
        LOWER(value) AS value_lower,
        SUBSTRING(value, 1, 2) AS value_prefix,
        COALESCE(value, 'DIRECT') AS value_filled
    FROM lines
    WHERE value IN ('BUNDLE', 'SPRING10', 'VIP')
),
kept_keys AS (
    SELECT customer_id, id
    FROM valid
    EXCEPT ALL
    SELECT customer_id, id
    FROM carts
    WHERE customer_id IN (2, 35)
),
reconciled AS (
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
joined AS (
    SELECT src.*, b.name
    FROM reconciled src
    LEFT JOIN customers b
        ON src.customer_id <=> b.id
),
valid_carts AS (
    SELECT
        id,
        customer_id,
        value,
        value_lower,
        value_prefix,
        value_filled,
        COALESCE(name, 'Unknown') AS name_filled,
        COALESCE(value, 'UNKNOWN') AS resolved_value
    FROM joined
    WHERE value = 'BUNDLE'
),
cleaned AS (
    SELECT
        id,
        customer_id,
        value,
        value_lower,
        value_prefix,
        value_filled,
        name_filled,
        resolved_value,
        COALESCE(value, 'Unknown') AS value_filled_2,
        CASE WHEN value = 'WELCOME5' THEN 1 ELSE 0 END AS is_welcome5,
        COALESCE(value, 'NONE') AS value_clean
    FROM valid_carts
    WHERE value IS NOT DISTINCT FROM 'BUNDLE'
),
joined_carts AS (
    SELECT
        e.*,
        c.segment,
        c.country
    FROM cleaned e
    INNER JOIN customers c
        ON e.customer_id = c.id
)
SELECT *
FROM (
    SELECT
        value,
        segment,
        is_welcome5
    FROM joined_carts
) src
PIVOT (
    AVG(is_welcome5)
    FOR segment IN ('Corporate', 'Consumer')
)
