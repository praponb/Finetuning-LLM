SELECT
    product_id,
    MAX(revenue) - MIN(revenue) AS revenue_range,
    MAX(sale_date) AS latest_sale_date,
    MAX(units) - MIN(units) AS units_range
FROM store_sales
WHERE product_id IN (1, 4, 6, 9) AND revenue <= 3000
GROUP BY product_id
HAVING MAX(revenue) - MIN(revenue) > 59;
