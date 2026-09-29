SELECT
    src.id,
    src.store_id,
    src.product_id,
    d.product_name,
    d.stock_qty
FROM inventory src
LEFT JOIN products d
    ON src.product_id = d.id
WHERE d.supplier_id = 27;
