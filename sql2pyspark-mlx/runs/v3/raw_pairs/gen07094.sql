SELECT
    id,
    category,
    brand,
    stock_qty,
    RANK() OVER (PARTITION BY category, brand ORDER BY stock_qty DESC) AS rnk
FROM products
WHERE UPPER(product_name) LIKE '%SB-%'
ORDER BY brand;
