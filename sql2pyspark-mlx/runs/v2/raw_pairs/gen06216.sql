select customer_id, account_type, count(balance) as balance_count, min(opened_date) as min_opened_date from accounts where month(opened_date) in (1, 6, 10) group by customer_id, account_type
