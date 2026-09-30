SELECT
    ss.store_id,
    ss.sale_date,
    p.supplier_id,
    p.price,
    p.launch_date
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
