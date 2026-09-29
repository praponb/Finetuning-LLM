WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, attr_value
)
SELECT
    id,
    created_at,
    k,
    attr_value
FROM lines
ORDER BY k NULLS FIRST
