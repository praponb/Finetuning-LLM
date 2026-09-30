SELECT
    s.sqft,
    s.region,
    ss.units,
    ss.sale_date,
    DATE_TRUNC('month', ss.sale_date) AS sale_date_period
FROM stores s
LEFT JOIN store_sales ss ON ss.store_id = s.id
WHERE ss.product_id IN (3, 4, 9)
