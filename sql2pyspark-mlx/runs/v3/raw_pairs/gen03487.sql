SELECT
    ss.units,
    s.store_name,
    p.launch_date,
    p.price
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
FULL OUTER JOIN products p ON p.id = ss.product_id
WHERE s.opened_date <= '2023-11-01';
