WITH exploded AS (
    SELECT
        id,
        created_at,
        code
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) x AS code
),
base AS (
    SELECT
        *,
        LOWER(code) AS code_lower,
        DATE_FORMAT(created_at, 'MMM yyyy') AS created_at_label
    FROM exploded
    WHERE created_at > DATE '2025-03-02'
      AND code IS NOT NULL
),
filtered AS (
    SELECT
        *,
        UPPER(code) AS code_upper,
        COALESCE(code, 'NONE') AS resolved_code
    FROM base
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 17:00:00')
      AND code IS NOT NULL
),
base_carts AS (
    SELECT
        *,
        INITCAP(TRIM(code)) AS code_title,
        COALESCE(code, 'UNKNOWN') AS code_clean
    FROM filtered
    WHERE created_at >= DATE '2025-03-01'
      AND code IN ('SPRING10', 'WELCOME5', 'BUNDLE')
),
kept_keys AS (
    SELECT id
    FROM base_carts
    EXCEPT
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
),
verified AS (
    SELECT s.*
    FROM base_carts s
    JOIN kept_keys k ON s.id = k.id
),
filtered_carts AS (
    SELECT *, LOWER(code) AS code_lower_2
    FROM verified
    WHERE code IS NOT NULL
      AND created_at >= '2025-03-01'
),
filtered_carts_2 AS (
    SELECT *, CASE WHEN code IN ('BUNDLE', 'SPRING10') THEN 'Core' ELSE 'Other' END AS code_group
    FROM filtered_carts
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 18:00:00')
      AND code IS NOT NULL
)
SELECT
    code,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    SIZE(COLLECT_SET(code)) AS num_codes,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(CASE WHEN code = 'WELCOME5' THEN 1 END) AS welcome5_count,
    SIZE(COLLECT_SET(code)) AS num_codes_2,
    SORT_ARRAY(COLLECT_SET(code)) AS code_list,
    SIZE(COLLECT_SET(code)) AS num_codes_3
FROM filtered_carts_2
GROUP BY GROUPING SETS ((code, DATE_TRUNC('DAY', created_at)), (code))
HAVING COUNT(DISTINCT id) > 1;
