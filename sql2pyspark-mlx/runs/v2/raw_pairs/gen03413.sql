SELECT
    i.id,
    i.quantity,
    i.product_id,
    s.sqft,
    s.store_name,
    p.launch_date,
    p.product_name,
    ADD_MONTHS(i.last_restock, 1) last_restock_plus_months
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON p.id = i.product_id;
