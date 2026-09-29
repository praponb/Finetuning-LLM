SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date DESC, id) AS row_num
    FROM products
    WHERE stock_qty BETWEEN 267 AND 353
) AS ranked
WHERE row_num <= 5
ORDER BY id DESC, cost DESC;
