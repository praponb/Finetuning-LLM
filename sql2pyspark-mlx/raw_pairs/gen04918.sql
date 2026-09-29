SELECT CASE WHEN c.segment IN ('Corporate', 'SMB') THEN 'priority' ELSE 'other' END AS segment_group, COUNT(o.discount) AS discount_count
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON o.customer_id = c.id
WHERE c.credit_limit >= 4500
GROUP BY CASE WHEN c.segment IN ('Corporate', 'SMB') THEN 'priority' ELSE 'other' END
ORDER BY segment_group, 2 DESC
