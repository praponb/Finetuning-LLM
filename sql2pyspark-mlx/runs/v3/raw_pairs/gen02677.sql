WITH base AS (
    SELECT *, CAST(is_active AS INT) AS is_active_flag
    FROM customers
    WHERE segment = 'Consumer' AND email IS NOT NULL AND city IS NOT NULL
),
customers_stats AS (
    SELECT
        is_active,
        country,
        COUNT(*) AS cnt,
        COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count
    FROM base
    GROUP BY is_active, country
)
SELECT
    is_active,
    country,
    cnt,
    consumer_count
FROM customers_stats
ORDER BY cnt DESC, is_active, country
LIMIT 50
