WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY brand ORDER BY launch_date DESC) AS row_num
    FROM products
    WHERE stock_qty >= 371
)
SELECT
    id,
    brand,
    launch_date,
    stock_qty
FROM numbered
WHERE row_num <= 3;
