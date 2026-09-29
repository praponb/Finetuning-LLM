SELECT
    DATE_TRUNC('MONTH', s.opened_date) AS opened_month,
    CASE WHEN s.sqft >= 16806 THEN 'high' WHEN s.sqft >= 2527 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    ROUND(STDDEV(i.quantity), 2) AS stddev_quantity,
    MAX(i.quantity) AS max_quantity
FROM inventory i
JOIN stores s ON s.id = i.store_id
LEFT JOIN products p ON i.product_id = p.id
WHERE s.sqft BETWEEN 6596 AND 8521
GROUP BY DATE_TRUNC('MONTH', s.opened_date), CASE WHEN s.sqft >= 16806 THEN 'high' WHEN s.sqft >= 2527 THEN 'mid' ELSE 'low' END
