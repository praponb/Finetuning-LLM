SELECT
    i.id,
    i.product_id,
    i.last_restock,
    p.brand,
    ADD_MONTHS(i.last_restock, 3) AS last_restock_plus_months
FROM inventory i
RIGHT JOIN products p ON i.product_id = p.id
ORDER BY product_id;
