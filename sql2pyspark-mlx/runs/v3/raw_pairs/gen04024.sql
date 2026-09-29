SELECT
    ss.sale_date,
    p.cost,
    p.launch_date,
    CASE WHEN p.brand IN ('Stark', 'Acme') THEN 'Core' ELSE 'Other' END AS brand_group
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
WHERE NOT (p.category <> 'Books' AND p.category = 'Fashion');
