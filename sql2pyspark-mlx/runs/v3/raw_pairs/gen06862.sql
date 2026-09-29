SELECT
    CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket,
    CASE WHEN city IN ('Chiang Mai', 'Bangkok') THEN 'group_a' ELSE 'other' END AS city_group,
    ROUND((100 * SUM(CASE WHEN country = 'US' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_us,
    MAX(rating) - MIN(rating) AS rating_range
FROM suppliers
WHERE country IN ('US', 'TH', 'UK') AND (country = 'UK' OR city IN ('New York', 'Bangkok'))
GROUP BY CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END, CASE WHEN city IN ('Chiang Mai', 'Bangkok') THEN 'group_a' ELSE 'other' END
ORDER BY pct_us DESC, rating_bucket, city_group
LIMIT 3
