SELECT
    id,
    price,
    stock_qty,
    cost,
    price + stock_qty AS price_plus_stock_qty
FROM products
WHERE price >= 840;
