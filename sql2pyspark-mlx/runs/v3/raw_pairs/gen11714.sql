SELECT
    i.id,
    i.quantity,
    i.product_id,
    p.supplier_id,
    p.stock_qty,
    CONCAT(p.product_name, ' - ', p.brand) AS product_name_brand
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
ORDER BY i.quantity DESC, id
LIMIT 10;
