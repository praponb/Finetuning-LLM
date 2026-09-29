SELECT AVG(discount) AS avg_discount FROM orders WHERE shipping_fee >= 20 ORDER BY avg_discount DESC
