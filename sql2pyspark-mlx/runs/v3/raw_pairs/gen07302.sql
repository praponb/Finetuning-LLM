SELECT customer_id, country FROM sessions WHERE country IN ('JP', 'US') UNION SELECT customer_id, country FROM sessions WHERE browser IN ('Edge', 'Chrome', 'Safari');
