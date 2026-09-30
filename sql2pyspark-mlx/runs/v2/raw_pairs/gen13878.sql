SELECT amount, ROUND(amount * 0.85, 2) AS amount_scaled FROM transactions WHERE HOUR(txn_time) BETWEEN 6 AND 9 OR account_id IN (11, 13, 32) ORDER BY amount_scaled DESC, amount;
