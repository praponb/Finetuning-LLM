SELECT
    i.quantity,
    p.launch_date,
    p.product_name,
    s.city,
    s.region,
    FLOOR(s.sqft) AS sqft_whole
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
LEFT JOIN stores s ON i.store_id = s.id
WHERE s.region IN ('East', 'Central')
ORDER BY launch_date;
