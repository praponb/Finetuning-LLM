select sum(balance) total_balance, round(avg(balance), 2) avg_balance from accounts where customer_id in (11, 17, 34, 38);
