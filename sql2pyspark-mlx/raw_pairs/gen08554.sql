WITH exploded AS (
    SELECT
        id,
        created_at,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS code
),
filtered AS (
    SELECT *, SUBSTRING(code, 1, 2) AS code_prefix
    FROM exploded
    WHERE created_at > TIMESTAMP '2025-03-02 00:00:00'
),
cleaned AS (
    SELECT *, UPPER(code) AS code_upper
    FROM filtered
    WHERE created_at > DATE '2025-03-02'
),
valid AS (
    SELECT
        *,
        INITCAP(TRIM(code)) AS code_title,
        HOUR(created_at) AS created_at_hour
    FROM cleaned
    WHERE created_at >= TO_DATE('2025-03-01')
      AND code != 'WELCOME5'
)
SELECT
    code,
    code_prefix,
    code_upper,
    code_title,
    created_at_hour,
    CASE WHEN code RLIKE '^(SPRING10|WELCOME5)$' THEN 'EXTENDED' ELSE 'CORE' END AS code_zone
FROM valid
ORDER BY code_zone;
