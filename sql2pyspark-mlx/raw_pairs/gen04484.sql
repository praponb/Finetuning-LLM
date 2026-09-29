select id, balance from accounts where balance < (select min(balance) from accounts where customer_id in (25, 32));
