SELECT c.id, c.age
FROM customers c
WHERE c.country NOT IN ('UK', 'JP') AND EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.country = 'TH')
