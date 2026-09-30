select id, balance from accounts where balance = (select min(balance) from accounts where customer_id in (10, 12, 14, 29));
