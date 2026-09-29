SELECT p.brand, MAX(i.quantity) - MIN(i.quantity) quantity_range
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON s.id = i.store_id
WHERE s.sqft > 11310
GROUP BY p.brand
ORDER BY p.brand ASC;
