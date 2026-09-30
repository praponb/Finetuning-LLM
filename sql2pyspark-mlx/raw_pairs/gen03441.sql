SELECT
    store_id,
    DATE_TRUNC('MONTH', sale_date) AS sale_month,
    product_id,
    MAX(units) AS max_units
FROM store_sales
WHERE store_id = 3 AND product_id IN (7, 8)
GROUP BY store_id, DATE_TRUNC('MONTH', sale_date), product_id
ORDER BY max_units, store_id;
