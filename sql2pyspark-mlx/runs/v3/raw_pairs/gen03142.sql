SELECT
    store_id,
    quantity,
    last_restock,
    MONTH(last_restock) AS last_restock_month,
    LAST_DAY(last_restock) AS last_restock_month_end
FROM inventory
WHERE (last_restock >= '2023-11-01' AND quantity IS NULL) OR last_restock > '2023-09-01'
ORDER BY quantity DESC;
