SELECT s.region, s.city, SUM(i.quantity) total_quantity
FROM inventory i
JOIN stores s ON i.store_id = s.id
LEFT OUTER JOIN products p ON i.product_id = p.id
WHERE p.stock_qty BETWEEN 136 AND 206
GROUP BY s.region, s.city;
