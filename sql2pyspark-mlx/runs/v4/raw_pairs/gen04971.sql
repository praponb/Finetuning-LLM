SELECT
    id,
    sale_date,
    revenue,
    DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label
FROM store_sales
WHERE product_id IN (4, 5, 7, 8) AND store_id = 5
ORDER BY revenue DESC;
