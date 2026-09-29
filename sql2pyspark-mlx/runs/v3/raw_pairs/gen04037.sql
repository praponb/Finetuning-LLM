select t.id, t.account_id, t.merchant, a.account_type, a.status from transactions t right join accounts a on t.account_id = a.id where a.balance >= 25000;
