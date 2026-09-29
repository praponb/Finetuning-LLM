WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id ASC) AS row_seq,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share
    FROM suppliers
)
-- Grouped metrics
SELECT
    CASE WHEN rating >= 4.1 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
    CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    city,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SUM(CASE WHEN city = 'Chiang Mai' THEN rating_share ELSE 0 END) AS chiang_mai_rating_share,
    COUNT(DISTINCT city) AS distinct_cities,
    AVG(rating) FILTER (WHERE country = 'JP') AS jp_avg_rating,
    ROUND(AVG(rating_share), 2) AS avg_rating_share,
    MAX(rating_share) FILTER (WHERE city = 'Bangkok') AS bangkok_max_rating_share
FROM metrics
GROUP BY CASE WHEN rating >= 4.1 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END, city;
