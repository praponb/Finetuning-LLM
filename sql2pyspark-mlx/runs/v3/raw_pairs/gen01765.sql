SELECT
    i.id,
    i.product_id,
    i.store_id,
    i.last_restock,
    ss.store_id store_sale_store_id,
    ss.revenue
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units BETWEEN 25 AND 40
ORDER BY revenue;
