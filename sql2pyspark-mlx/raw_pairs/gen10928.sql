select a.account_type, a.opened_date, t.merchant, t.account_id, t.is_flagged from accounts a inner join transactions t on t.account_id = a.id where a.customer_id is null;
