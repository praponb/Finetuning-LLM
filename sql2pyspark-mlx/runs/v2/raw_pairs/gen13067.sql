SELECT
    store_sales.id,
    store_sales.units,
    stores.city,
    LEAST(store_sales.units, store_sales.revenue) AS least_units_revenue
FROM store_sales
INNER JOIN stores ON store_sales.store_id = stores.id
WHERE store_sales.store_id = 6
