SELECT merchant, MAX(amount) - MIN(amount) AS amount_range
FROM transactions
GROUP BY merchant;
