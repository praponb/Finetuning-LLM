SELECT merchant, SUM(amount) AS total_amount, COUNT(CASE WHEN merchant = 'Lazada' THEN 1 END) AS lazada_count FROM transactions GROUP BY merchant;
