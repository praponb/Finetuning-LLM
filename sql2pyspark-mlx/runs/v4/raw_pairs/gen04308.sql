SELECT
    store_id,
    product_id,
    YEAR(sale_date) AS sale_year,
    MAX(sale_date) AS last_sale_date
FROM store_sales
WHERE MONTH(sale_date) IN (1, 5, 7) AND (revenue > 2100 OR sale_date BETWEEN '2023-05-01' AND '2023-08-01')
GROUP BY store_id, product_id, YEAR(sale_date)
