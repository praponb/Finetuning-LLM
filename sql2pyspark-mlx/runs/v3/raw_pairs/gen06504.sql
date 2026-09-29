SELECT
    stores.store_name,
    stores.opened_date,
    t.unique_products,
    t.min_revenue
FROM stores
LEFT OUTER JOIN (SELECT store_id, COUNT(DISTINCT revenue) AS unique_products, MIN(revenue) AS min_revenue
    FROM store_sales
    GROUP BY store_id) t ON stores.id = t.store_id
WHERE t.min_revenue > 3200
ORDER BY t.unique_products DESC, store_name DESC
