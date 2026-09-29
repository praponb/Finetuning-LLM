WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC NULLS LAST, id) = 1
)
-- Multi-level aggregation
SELECT
    country,
    ROUND(AVG(rating), 2) AS avg_rating,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS rating_per_london,
    MAX(rating) AS max_rating,
    SUM(rating) FILTER (WHERE city = 'Bangkok') AS bangkok_total_rating,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count
FROM deduped
GROUP BY country
HAVING max_rating > 0;
