WITH Src AS (
    SELECT *
    FROM suppliers
    WHERE rating > 2.6
),
summary AS (
    SELECT
        CASE WHEN country IN ('UK', 'DE') THEN 'group_a' ELSE 'other' END AS country_group,
        CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END AS rating_bucket,
        ROUND((100 * SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_london,
        AVG(rating) AS avg_rating
    FROM Src
    GROUP BY CASE WHEN country IN ('UK', 'DE') THEN 'group_a' ELSE 'other' END, CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
WHERE avg_rating < 4.2
ORDER BY pct_london DESC, country_group
