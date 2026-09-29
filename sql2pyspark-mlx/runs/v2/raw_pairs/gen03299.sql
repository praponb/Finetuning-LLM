SELECT is_active, segment
FROM customers
WHERE is_active AND country IN ('UK', 'DE', 'SG')
ORDER BY segment DESC
