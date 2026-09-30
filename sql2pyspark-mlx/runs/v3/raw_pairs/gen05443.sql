SELECT id, amount, RANK() OVER (ORDER BY amount) AS rnk FROM orders;
