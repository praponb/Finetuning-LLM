SELECT merchant, ROUND(STDDEV(amount), 2) AS stddev_amount, COUNT(*) AS cnt FROM transactions WHERE txn_type = 'CREDIT' AND txn_time >= '2025-04-01' GROUP BY merchant ORDER BY merchant;
