WITH lines AS (
    SELECT
        id,
        attr_key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, value
),
valid AS (
    SELECT *, COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM lines
    WHERE attr_key IS NOT NULL
),
kept_keys AS (
    SELECT id
    FROM valid
    INTERSECT
    SELECT id
    FROM carts
    WHERE created_at < '2025-01-01'
),
verified AS (
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.id = k.id
),
base AS (
    SELECT
        *,
        INITCAP(TRIM(attr_key)) AS attr_key_title,
        COALESCE(attr_key, 'Unknown') AS attr_key_filled_2
    FROM verified
    WHERE attr_key IN ('device', 'utm_source')
),
filtered AS (
    SELECT
        id,
        value,
        attr_key_title,
        attr_key_filled_2,
        UPPER(attr_key) AS attr_key_upper
    FROM base
    WHERE attr_key IN ('weight_kg', 'device', 'utm_source')
)
SELECT id
FROM filtered;
