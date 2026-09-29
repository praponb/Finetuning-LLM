SELECT
    MONTH(s.opened_date) AS opened_month,
    CASE WHEN s.sqft >= 17868 THEN 'high' WHEN s.sqft >= 4183 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.status))) AS status_values,
    MIN(p.amount) AS min_amount
FROM payments p
JOIN orders o ON p.order_id = o.id
LEFT JOIN stores s ON o.store_id = s.id
WHERE p.status = 'FAILED'
GROUP BY MONTH(s.opened_date), CASE WHEN s.sqft >= 17868 THEN 'high' WHEN s.sqft >= 4183 THEN 'mid' ELSE 'low' END
