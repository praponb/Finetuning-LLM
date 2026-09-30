select t.amount, a.status, a.account_type, a.balance from transactions t left join accounts a on t.account_id = a.id where t.is_flagged;
