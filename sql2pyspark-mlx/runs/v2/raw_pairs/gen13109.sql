SELECT *
FROM (
    SELECT brand, MAX(stock_qty) max_stock_qty
    FROM products
    GROUP BY brand
) sub
WHERE max_stock_qty < 210;
