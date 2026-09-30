WITH store_totals AS (
    SELECT store_id, COUNT(DISTINCT revenue) unique_products
    FROM store_sales
    GROUP BY store_id
)
SELECT
    stores.id,
    stores.opened_date,
    stores.store_name,
    store_totals.unique_products
FROM stores
JOIN store_totals ON stores.id = store_totals.store_id
WHERE store_totals.unique_products >= 4
ORDER BY store_totals.unique_products, id DESC
