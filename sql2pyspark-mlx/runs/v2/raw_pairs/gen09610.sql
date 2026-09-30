select customer_id, country from sessions where customer_id is null union select customer_id, country from sessions where country != 'US'
