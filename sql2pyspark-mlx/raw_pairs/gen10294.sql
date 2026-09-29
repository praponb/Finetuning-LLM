SELECT
    id,
    product_name,
    price,
    stock_qty
FROM products
QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC NULLS FIRST, id) = 1;
