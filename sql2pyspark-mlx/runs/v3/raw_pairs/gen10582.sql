SELECT merchant, SUM(amount) AS total_amount, ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total
FROM transactions
WHERE merchant IN ('7-Eleven', 'Shell')
GROUP BY merchant
ORDER BY merchant;
