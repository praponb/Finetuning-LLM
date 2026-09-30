SELECT *
FROM sessions
WHERE browser = 'Safari' OR customer_id IS NOT NULL;
