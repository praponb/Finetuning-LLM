SELECT brand, unique_suppliers, avg_price
FROM (
    SELECT brand, COUNT(DISTINCT stock_qty) unique_suppliers, ROUND(AVG(price), 2) avg_price
    FROM products
    GROUP BY brand
) t
WHERE avg_price < 79;
