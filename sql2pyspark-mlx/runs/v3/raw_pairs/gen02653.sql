SELECT TO_DATE(txn_time) AS txn_day, merchant, ROUND(AVG(amount), 2) AS avg_amount, COUNT(DISTINCT amount) AS unique_accounts FROM transactions GROUP BY txn_day, merchant ORDER BY merchant;
