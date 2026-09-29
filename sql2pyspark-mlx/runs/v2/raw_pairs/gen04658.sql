SELECT id, customer_id, balance FROM accounts WHERE status IN ('OPEN', 'FROZEN') AND MONTH(opened_date) IN (3, 7, 12) AND account_type NOT IN ('LOAN', 'CHECKING') ORDER BY 2 DESC;
