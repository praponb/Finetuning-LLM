SELECT t.id, t.account_id, a.opened_date, a.customer_id, a.balance FROM transactions t INNER JOIN accounts a ON a.id = t.account_id WHERE MONTH(a.opened_date) IN (2, 7, 10) AND a.balance >= 22000;
