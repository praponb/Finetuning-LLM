SELECT
    country,
    segment,
    pct_london,
    total_count
FROM (
    SELECT
        country,
        segment,
        ROUND((100 * SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_london,
        COUNT(*) AS total_count
    FROM customers
    WHERE country NOT IN ('SG', 'DE')
    GROUP BY country, segment
) AS s
WHERE total_count < 6
