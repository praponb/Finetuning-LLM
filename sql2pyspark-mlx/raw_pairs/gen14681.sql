SELECT merchant, AVG(amount) AS avg_amount FROM transactions WHERE txn_type <> 'CREDIT' AND is_flagged GROUP BY merchant HAVING AVG(amount) >= 1400 ORDER BY avg_amount DESC;
