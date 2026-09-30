SELECT
    id,
    stock_qty,
    product_name,
    tags,
    cost - stock_qty AS cost_minus_stock_qty
FROM products
ORDER BY id DESC, product_name
