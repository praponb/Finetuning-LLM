WITH base AS (
    SELECT
        *,
        SUBSTRING(name, 1, 4) AS name_prefix,
        COALESCE(segment, 'DIRECT') AS resolved_segment
    FROM customers
    WHERE city = 'Chiang Mai'
)
SELECT
    id,
    email,
    name_prefix,
    LAST_VALUE(age) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('month', signup_date), country ORDER BY credit_limit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_age,
    ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share
FROM base
WHERE email IS NOT NULL;
