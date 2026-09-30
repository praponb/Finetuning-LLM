SELECT
    i.id,
    i.last_restock,
    ss.sale_date,
    ss.product_id,
    LAST_DAY(i.last_restock) AS last_restock_month_end
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY last_restock_month_end DESC;
