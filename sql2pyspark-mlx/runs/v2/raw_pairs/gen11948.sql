SELECT store_id, SUM(discount) AS total_discount, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(discount) DESC) AS discount_rank FROM orders WHERE shipping_fee IS NOT NULL GROUP BY store_id;
