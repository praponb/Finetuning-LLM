SELECT
    i.quantity,
    p.product_name,
    s.city,
    s.store_name,
    CASE WHEN s.sqft >= 14608 THEN 'High' WHEN s.sqft >= 5574 THEN 'Medium' ELSE 'Low' END AS sqft_band
FROM inventory i
RIGHT JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON s.id = i.store_id;
