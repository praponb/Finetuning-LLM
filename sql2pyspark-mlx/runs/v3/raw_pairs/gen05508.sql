SELECT
    CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket,
    COUNT(*) AS supplier_count,
    SUM(rating) AS total_rating,
    SUM(CASE WHEN country = 'TH' THEN rating ELSE 0 END) AS th_rating
FROM suppliers
WHERE city = 'Singapore' AND city IS NULL
GROUP BY 1;
