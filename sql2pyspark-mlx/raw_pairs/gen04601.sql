SELECT
    i.id,
    i.quantity,
    i.product_id,
    i.last_restock,
    ss.product_id AS store_sale_product_id,
    ss.revenue,
    ss.units,
    DATEDIFF(CURRENT_DATE(), i.last_restock) / 7 AS weeks_since_last_restock
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY weeks_since_last_restock;
