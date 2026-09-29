SELECT
    i.product_id,
    i.last_restock,
    i.quantity,
    ss.store_id,
    ss.product_id AS store_sale_product_id,
    ss.units,
    ROUND(i.quantity * 1.15, 2) AS quantity_adjusted
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.last_restock BETWEEN '2023-09-01' AND '2025-03-28'
ORDER BY last_restock ASC
