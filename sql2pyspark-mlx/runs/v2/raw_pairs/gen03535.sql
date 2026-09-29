SELECT merchant, ROUND(STDDEV(amount), 2) AS stddev_amount, SUM(amount) AS total_amount, MAX(amount) AS max_amount FROM transactions GROUP BY 1 HAVING SUM(amount) < 23000 ORDER BY merchant;
