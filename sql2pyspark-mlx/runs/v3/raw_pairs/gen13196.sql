SELECT id, brand, price, SUM(price) OVER (PARTITION BY brand) AS brand_sum_price FROM products;
