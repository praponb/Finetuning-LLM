SELECT t.id, t.merchant, a.customer_id, UPPER(a.account_type) AS account_type_upper FROM transactions t INNER JOIN accounts a ON t.account_id = a.id WHERE t.account_id IN (20, 28, 37, 38);
