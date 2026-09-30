SELECT
    i.quantity,
    s.region,
    p.stock_qty,
    LAST_DAY(i.last_restock) AS last_restock_month_end
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON p.id = i.product_id
WHERE p.stock_qty IS NULL
ORDER BY s.region DESC, i.quantity DESC;
