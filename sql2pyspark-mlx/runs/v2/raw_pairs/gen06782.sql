SELECT
    stores.region,
    QUARTER(stores.opened_date) AS opened_quarter,
    ROUND(STDDEV(store_sales.units), 2) AS stddev_units,
    MIN(store_sales.revenue) AS min_revenue
FROM store_sales
INNER JOIN products ON products.id = store_sales.product_id
INNER JOIN stores ON store_sales.store_id = stores.id
WHERE store_sales.store_id IN (1, 4, 6)
GROUP BY stores.region, QUARTER(stores.opened_date)
HAVING MIN(store_sales.revenue) > 2300
