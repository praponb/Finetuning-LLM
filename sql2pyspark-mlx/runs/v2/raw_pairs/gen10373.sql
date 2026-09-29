SELECT
    ss.id,
    ss.store_id,
    p.product_name,
    p.launch_date,
    p.brand
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id
WHERE ss.units BETWEEN 20 AND 36;
