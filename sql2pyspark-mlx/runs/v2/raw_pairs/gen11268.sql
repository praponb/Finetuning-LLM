SELECT brand, SUM(price) total_price, DENSE_RANK() OVER (ORDER BY SUM(price) DESC) AS price_rank
FROM products
GROUP BY brand;
