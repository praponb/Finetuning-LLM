SELECT
    p.stock_qty,
    i.quantity,
    i.product_id,
    i.store_id,
    DATEDIFF(CURRENT_DATE(), i.last_restock) AS days_since_last_restock
FROM products p
LEFT JOIN inventory i ON p.id = i.product_id
WHERE p.stock_qty = 395;
