SELECT
    ss.id,
    ss.sale_date,
    ss.store_id,
    ss.product_id,
    s.sqft,
    s.city
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
ORDER BY 3 DESC;
