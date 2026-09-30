SELECT
    i.id,
    i.last_restock,
    i.product_id,
    p.cost,
    p.launch_date,
    s.opened_date
FROM inventory i
RIGHT OUTER JOIN products p ON i.product_id = p.id
FULL OUTER JOIN stores s ON i.store_id = s.id
ORDER BY p.cost ASC, last_restock ASC;
