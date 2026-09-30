SELECT *
FROM (
    SELECT
        id,
        category,
        stock_qty,
        launch_date,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty, id) row_num
    FROM products
) ranked
WHERE row_num <= 2
