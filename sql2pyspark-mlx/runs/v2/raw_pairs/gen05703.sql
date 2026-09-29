SELECT
    i.id,
    i.store_id,
    ss.units,
    LAST_DAY(i.last_restock) AS last_restock_month_end
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.last_restock <= '2025-10-01'
