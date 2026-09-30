SELECT customer_id, country FROM sessions WHERE browser IN ('Edge', 'Firefox', 'Safari') UNION ALL SELECT customer_id, country FROM sessions WHERE device = 'mobile' ORDER BY country;
