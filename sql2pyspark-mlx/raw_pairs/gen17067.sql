SELECT
    store_id,
    product_id,
    COUNT(*) AS total_count,
    AVG(units) AS avg_units,
    MAX(sale_date) AS last_sale_date
FROM store_sales
WHERE revenue BETWEEN 3400 AND 3900
GROUP BY store_id, product_id
HAVING AVG(units) >= 33;
