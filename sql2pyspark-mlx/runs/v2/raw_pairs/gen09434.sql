select id, country, browser from sessions where browser = 'Safari' union all select id, country, browser from sessions where customer_id = 5;
