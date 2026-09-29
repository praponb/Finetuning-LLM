SELECT id, customer_id
FROM sessions
EXCEPT DISTINCT
SELECT id, customer_id
FROM sessions
WHERE country = 'DE';
