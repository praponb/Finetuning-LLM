SELECT opened_date FROM accounts WHERE status = 'FROZEN' INTERSECT SELECT opened_date FROM accounts WHERE account_type IN ('SAVINGS', 'CREDIT') ORDER BY opened_date;
