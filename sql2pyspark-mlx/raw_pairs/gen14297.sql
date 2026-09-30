WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date, id) AS rnk
    FROM store_sales
    WHERE units >= 32
)
SELECT *
FROM ranked
WHERE rnk <= 3
ORDER BY product_id DESC
