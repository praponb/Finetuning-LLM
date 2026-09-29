SELECT
    i.id,
    i.quantity,
    p.stock_qty,
    p.supplier_id
FROM inventory i
FULL OUTER JOIN products p ON p.id = i.product_id;
