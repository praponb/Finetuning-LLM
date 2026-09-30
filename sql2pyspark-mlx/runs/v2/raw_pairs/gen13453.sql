SELECT
    p.id,
    p.stock_qty,
    p.price,
    t.avg_units,
    t.min_units
FROM products p
JOIN (SELECT product_id, ROUND(AVG(units), 2) AS avg_units, MIN(units) AS min_units
    FROM store_sales
    WHERE revenue <= 1600
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY price DESC
