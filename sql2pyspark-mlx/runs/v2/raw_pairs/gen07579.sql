SELECT p.brand, QUARTER(p.launch_date) AS launch_quarter, MAX(i.last_restock) AS last_last_restock
FROM inventory i
JOIN products p ON p.id = i.product_id
JOIN stores s ON i.store_id = s.id
WHERE LENGTH(s.store_name) > 12
GROUP BY p.brand, QUARTER(p.launch_date)
