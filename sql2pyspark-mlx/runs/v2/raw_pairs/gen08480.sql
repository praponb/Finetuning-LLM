SELECT account_id, amount_count FROM (SELECT account_id, COUNT(amount) AS amount_count FROM transactions GROUP BY account_id) AS agg WHERE amount_count >= 9 ORDER BY account_id;
