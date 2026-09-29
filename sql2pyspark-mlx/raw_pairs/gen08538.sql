SELECT
    id,
    last_restock,
    quantity,
    ADD_MONTHS(last_restock, 3) AS last_restock_plus_months
FROM inventory
WHERE NOT (store_id IN (1, 2, 5) AND last_restock < '2024-03-01')
ORDER BY quantity;
