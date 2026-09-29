SELECT stores.region, ROUND(STDDEV(store_sales.units), 2) AS stddev_units, MAX(store_sales.sale_date) AS last_sale_date
FROM store_sales
JOIN stores ON store_sales.store_id = stores.id
GROUP BY stores.region
ORDER BY stddev_units DESC, region
LIMIT 10
