SELECT id, customer_id, browser
FROM sessions
WHERE device = 'desktop'
EXCEPT
SELECT id, customer_id, browser
FROM sessions
WHERE country != 'UK'
ORDER BY id DESC
