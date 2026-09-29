SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC, id) AS row_num
    FROM products
    WHERE cost > 280
) AS r
WHERE row_num <= 3
ORDER BY brand DESC
