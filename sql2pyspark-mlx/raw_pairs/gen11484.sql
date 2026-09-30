select customer_id, min(balance) as min_balance, sum(case when account_type = 'CHECKING' then 1 else 0 end) as checking_count from accounts group by customer_id order by customer_id desc limit 50
