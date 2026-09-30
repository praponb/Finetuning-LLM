SELECT p.id, p.brand, COUNT(i.id) AS inventory_count
FROM products p
LEFT OUTER JOIN inventory i ON i.product_id = p.id
WHERE p.launch_date >= '2023-05-14'
GROUP BY p.id, p.brand
ORDER BY p.id;
