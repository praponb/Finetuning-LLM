select sessions.id, sessions.browser from sessions where sessions.customer_id not in (select id from customers where city in ('Bangkok', 'Berlin'));
