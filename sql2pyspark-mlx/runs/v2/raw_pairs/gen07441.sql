SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS rnk
    FROM store_sales
) AS ranked
WHERE rnk = 1;
