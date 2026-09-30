SELECT
    CASE WHEN rating >= 3.9 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END rating_bucket,
    MIN(rating) min_rating,
    AVG(rating) avg_rating,
    COUNT(*) supplier_count
FROM suppliers
GROUP BY rating_bucket
HAVING MIN(rating) >= 2.1;
