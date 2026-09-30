select a.status, count(t.id) as transaction_count, min(t.amount) as min_amount, count(t.amount) as amount_count from accounts a join transactions t on t.account_id = a.id group by a.status;
