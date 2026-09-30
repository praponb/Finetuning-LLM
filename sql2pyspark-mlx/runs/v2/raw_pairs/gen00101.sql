SELECT brand, SUM(price) AS total_price, COUNT(*) AS cnt, SUM(SUM(price)) OVER (ORDER BY brand) AS cumulative_price FROM products WHERE brand = 'Stark' GROUP BY brand ORDER BY brand, cumulative_price
