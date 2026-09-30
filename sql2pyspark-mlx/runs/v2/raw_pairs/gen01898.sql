SELECT merchant, SUM(amount) AS total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
FROM transactions
GROUP BY merchant;
