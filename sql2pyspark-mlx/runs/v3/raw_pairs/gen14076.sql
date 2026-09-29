SELECT device, browser
FROM sessions
WHERE country = 'TH' AND customer_id IS NOT NULL;
