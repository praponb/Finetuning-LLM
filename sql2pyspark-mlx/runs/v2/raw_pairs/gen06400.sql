SELECT store_id, SUM(amount) total_amount, SUM(SUM(amount)) OVER (ORDER BY store_id) AS cumulative_amount
FROM orders
WHERE channel = 'partner'
GROUP BY store_id
ORDER BY cumulative_amount DESC;
