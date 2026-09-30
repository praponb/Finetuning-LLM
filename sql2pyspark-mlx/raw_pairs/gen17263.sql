SELECT CASE WHEN c.age >= 69 THEN 'high' WHEN c.age >= 27 THEN 'mid' ELSE 'low' END age_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(o.status))) status_values
FROM orders o
RIGHT JOIN customers c ON o.customer_id = c.id
WHERE o.channel IN ('partner', 'mobile', 'store')
GROUP BY CASE WHEN c.age >= 69 THEN 'high' WHEN c.age >= 27 THEN 'mid' ELSE 'low' END
ORDER BY status_values, age_bucket;
