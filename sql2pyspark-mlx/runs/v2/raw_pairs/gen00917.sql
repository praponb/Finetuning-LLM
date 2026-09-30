SELECT product_id, avg_units, min_sale_date
FROM (
    SELECT product_id, ROUND(AVG(units), 2) avg_units, MIN(sale_date) min_sale_date
    FROM store_sales
    GROUP BY product_id
) x
WHERE avg_units < 58
ORDER BY 3, product_id
LIMIT 10
