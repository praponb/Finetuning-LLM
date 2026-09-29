SELECT id, device
FROM sessions
WHERE device IN ('desktop', 'tablet', 'mobile') AND (customer_id IS NOT NULL OR browser = 'Firefox');
