SELECT p.brand, MIN(i.quantity) min_quantity
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON s.id = i.store_id
GROUP BY p.brand
ORDER BY brand ASC;
