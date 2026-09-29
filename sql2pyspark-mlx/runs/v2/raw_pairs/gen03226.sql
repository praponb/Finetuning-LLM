SELECT MAX(price) AS max_price, COUNT(DISTINCT cost) AS unique_suppliers
FROM products
ORDER BY unique_suppliers
