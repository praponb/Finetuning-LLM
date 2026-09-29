SELECT
    id,
    quantity,
    product_id,
    store_id,
    DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
FROM inventory
WHERE store_id IN (2, 5) AND MONTH(last_restock) IN (1, 8, 9)
ORDER BY store_id DESC, days_since_last_restock DESC;
