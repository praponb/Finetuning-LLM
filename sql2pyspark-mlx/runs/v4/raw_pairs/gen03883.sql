SELECT
    i.id,
    i.quantity,
    i.store_id,
    i.product_id,
    ss.sale_date,
    DATEDIFF(CURRENT_DATE(), i.last_restock) AS days_since_last_restock
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id;
