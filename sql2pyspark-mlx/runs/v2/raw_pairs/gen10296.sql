SELECT
    id,
    product_id,
    store_id,
    ADD_MONTHS(last_restock, 3) AS last_restock_plus_months
FROM inventory
WHERE last_restock BETWEEN '2024-10-01' AND '2025-01-01' AND quantity BETWEEN 62 AND 219 AND product_id IN (1, 4, 8, 10);
