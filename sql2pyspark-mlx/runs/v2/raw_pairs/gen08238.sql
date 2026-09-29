WITH Numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) rn
    FROM store_sales
)
SELECT *
FROM Numbered
WHERE rn <= 3;
