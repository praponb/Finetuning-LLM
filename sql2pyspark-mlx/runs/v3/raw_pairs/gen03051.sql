SELECT
    id,
    brand,
    launch_date,
    cost
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC, id) AS rn
    FROM products
    WHERE stock_qty != 148
) AS r
WHERE rn = 1
ORDER BY id DESC
