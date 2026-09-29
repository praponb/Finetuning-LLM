SELECT account_id, MAX(amount) - MIN(amount) AS amount_range FROM transactions GROUP BY account_id HAVING MAX(amount) - MIN(amount) < 49;
