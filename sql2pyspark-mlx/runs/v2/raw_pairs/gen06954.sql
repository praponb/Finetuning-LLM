SELECT *
FROM (
    SELECT
        id,
        product_id,
        sale_date,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) AS rnk
    FROM store_sales
) AS r
WHERE rnk <= 2
ORDER BY product_id DESC, rnk DESC;
