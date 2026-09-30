SELECT
    id,
    product_id,
    sale_date,
    revenue
FROM (
    SELECT
        id,
        product_id,
        sale_date,
        revenue,
        RANK() OVER (PARTITION BY product_id ORDER BY sale_date) AS rnk
    FROM store_sales
) AS t
WHERE rnk <= 1
ORDER BY sale_date;
