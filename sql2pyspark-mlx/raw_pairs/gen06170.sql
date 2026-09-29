SELECT i.store_id, p.supplier_id
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
WHERE p.launch_date BETWEEN '2024-03-01' AND '2024-07-22'
ORDER BY supplier_id DESC;
