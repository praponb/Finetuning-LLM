select t.account_id, t.amount, a.status, a.balance, lower(a.account_type) as account_type_lower from transactions t left join accounts a on t.account_id = a.id order by t.account_id;
