SELECT
    i.last_restock,
    p.cost,
    s.region,
    s.opened_date,
    YEAR(i.last_restock) AS last_restock_year
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
JOIN stores s ON s.id = i.store_id;
