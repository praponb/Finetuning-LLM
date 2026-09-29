SELECT
    id,
    store_id,
    last_restock,
    product_id,
    LAST_DAY(last_restock) AS last_restock_month_end
FROM inventory
WHERE YEAR(last_restock) = 2024
ORDER BY 3, id
LIMIT 10;
