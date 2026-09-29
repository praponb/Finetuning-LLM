SELECT
    i.last_restock,
    i.store_id,
    p.price,
    p.stock_qty,
    s.city,
    s.country,
    LAST_DAY(p.launch_date) AS launch_date_month_end
FROM inventory i
JOIN products p ON i.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id;
