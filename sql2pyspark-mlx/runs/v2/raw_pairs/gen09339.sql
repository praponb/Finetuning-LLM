SELECT
    product_id,
    store_id,
    MONTH(last_restock) AS last_restock_month,
    MIN(quantity) AS min_quantity
FROM inventory
WHERE NOT (quantity = 83 AND MONTH(last_restock) IN (4, 6, 11) AND quantity >= 189)
GROUP BY product_id, store_id, MONTH(last_restock)
HAVING MIN(quantity) < 160
ORDER BY store_id, product_id DESC;
