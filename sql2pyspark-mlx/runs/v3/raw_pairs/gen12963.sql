select status from orders union all select status from accounts where customer_id in (1, 6, 32, 38);
