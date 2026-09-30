SELECT
    i.id,
    i.last_restock,
    i.product_id,
    ss.store_id,
    LEAST(ss.revenue, ss.units) AS least_revenue_units
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units <> 15 AND i.store_id IN (1, 2)
ORDER BY ss.store_id DESC;
