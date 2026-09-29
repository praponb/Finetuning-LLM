WITH inventory_by_store AS (
    SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range, COUNT(DISTINCT quantity) AS unique_products
    FROM inventory
    WHERE product_id IN (4, 5, 8, 10)
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    ibs.quantity_range,
    ibs.unique_products
FROM stores s
LEFT OUTER JOIN inventory_by_store ibs ON s.id = ibs.store_id;
