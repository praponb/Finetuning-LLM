SELECT CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END) AS berlin_count, COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count
FROM suppliers
WHERE rating <= 3.6 AND (supplier_name LIKE '%Tokyo%' OR supplier_name LIKE 'G%')
GROUP BY CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END;
