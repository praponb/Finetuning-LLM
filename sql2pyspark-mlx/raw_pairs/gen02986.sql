select a.customer_id, min(t.amount) as min_amount from transactions t join accounts a on t.account_id = a.id group by a.customer_id;
