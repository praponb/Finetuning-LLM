SELECT email FROM customers WHERE country != 'DE' UNION SELECT email FROM customers WHERE country IN ('UK', 'TH', 'DE') ORDER BY email DESC;
