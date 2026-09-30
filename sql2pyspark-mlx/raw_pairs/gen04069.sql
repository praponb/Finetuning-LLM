SELECT
    id,
    category,
    stock_qty,
    RANK() OVER (PARTITION BY category ORDER BY stock_qty) rnk
FROM products
WHERE cost BETWEEN 350 AND 550
ORDER BY id DESC;
