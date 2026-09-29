WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS k, v
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(k) AS k_lower
    FROM flattened
    WHERE k IS NOT DISTINCT FROM 'gift'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(k, 1, 4) AS k_prefix,
        COALESCE(k, 'NONE') AS k_filled
    FROM filtered
    WHERE k IS NOT NULL
      AND k = 'gift'
),
filtered_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        k,
        k_lower,
        k_filled,
        INITCAP(TRIM(k)) AS k_title,
        LOWER(k) AS k_lower_2
    FROM valid
    WHERE k = 'gift'
)
SELECT
    id,
    k,
    k_title,
    CASE WHEN k RLIKE '^(device|utm_source)$' THEN 'CORE' ELSE 'EXTENDED' END AS k_zone
FROM filtered_carts
ORDER BY k_zone DESC NULLS FIRST;
