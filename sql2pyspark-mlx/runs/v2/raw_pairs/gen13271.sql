SELECT
    i.id,
    i.product_id,
    p.category,
    p.stock_qty,
    p.cost,
    LAST_DAY(i.last_restock) AS last_restock_month_end
FROM inventory i
LEFT JOIN products p ON p.id = i.product_id
ORDER BY product_id DESC;
