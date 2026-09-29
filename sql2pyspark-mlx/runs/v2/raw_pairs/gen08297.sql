select a.balance, a.customer_id, a.account_type, t.account_id, t.amount from accounts a left join transactions t on t.account_id = a.id order by account_type;
