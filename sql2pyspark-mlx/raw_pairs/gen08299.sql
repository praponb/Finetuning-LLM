SELECT id, merchant, amount, SUM(amount) OVER (PARTITION BY merchant) AS merchant_sum_amount FROM transactions ORDER BY merchant;
