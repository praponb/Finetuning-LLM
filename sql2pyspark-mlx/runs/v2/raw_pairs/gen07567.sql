SELECT
    store_id,
    product_id,
    ROUND(STDDEV(revenue), 2) AS stddev_revenue,
    COUNT(units) AS units_count
FROM store_sales
WHERE store_id IN (1, 2, 4, 6)
GROUP BY store_id, product_id;
