select country, 'supplier' as source from suppliers where city in ('Bangkok', 'New York') union all select country, 'session' as source from sessions where customer_id = 20;
