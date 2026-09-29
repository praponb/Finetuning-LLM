SELECT
    id,
    product_id,
    sale_date,
    store_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) AS rnk
    FROM store_sales
) AS ranked
WHERE rnk <= 3
