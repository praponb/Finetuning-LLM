select t.txn_type, a.customer_id from transactions t left outer join accounts a on a.id = t.account_id where a.balance < 68000;
