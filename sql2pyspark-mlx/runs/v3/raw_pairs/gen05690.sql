select a.customer_id, sum(t.amount) as total_amount from transactions t left join accounts a on t.account_id = a.id where not t.is_flagged group by a.customer_id
