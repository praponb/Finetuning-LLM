select id, balance, abs(balance) as abs_balance from accounts where customer_id in (8, 23, 30) order by balance desc;
