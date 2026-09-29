SELECT
    i.id,
    i.last_restock,
    i.product_id,
    i.quantity,
    ss.units,
    ss.store_id,
    ss.sale_date,
    DATEDIFF(CURRENT_DATE(), i.last_restock) days_since_last_restock
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units <= 20 AND i.store_id = 3
ORDER BY i.last_restock, days_since_last_restock DESC;
