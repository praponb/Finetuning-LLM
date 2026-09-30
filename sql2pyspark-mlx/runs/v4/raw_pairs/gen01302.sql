SELECT
    ss.store_id,
    s.sqft,
    s.city,
    p.launch_date,
    p.product_name,
    COALESCE(s.store_name, 'unassigned') AS store_name_filled
FROM store_sales ss
LEFT OUTER JOIN stores s ON ss.store_id = s.id
JOIN products p ON p.id = ss.product_id
WHERE ss.units < 27;
