SELECT * FROM (SELECT account_id, MAX(amount) - MIN(amount) AS amount_range FROM transactions GROUP BY account_id) AS t WHERE amount_range >= 52 ORDER BY amount_range, account_id;
