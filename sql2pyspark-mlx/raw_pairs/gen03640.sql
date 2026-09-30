SELECT browser, customer_id FROM sessions WHERE browser = 'Safari' UNION SELECT browser, customer_id FROM sessions WHERE country IN ('UK', 'US')
