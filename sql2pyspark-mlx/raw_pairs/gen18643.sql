SELECT
    i.id,
    i.store_id,
    i.last_restock,
    ss.units,
    ss.product_id,
    ss.sale_date,
    LAST_DAY(i.last_restock) AS last_restock_month_end
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id = 5 AND ss.units = 22
ORDER BY product_id DESC;
