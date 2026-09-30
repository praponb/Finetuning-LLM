SELECT
    i.id,
    i.quantity,
    s.store_name,
    ADD_MONTHS(i.last_restock, 1) AS last_restock_plus_months
FROM inventory i
JOIN stores s ON i.store_id = s.id
WHERE s.region = 'West'
