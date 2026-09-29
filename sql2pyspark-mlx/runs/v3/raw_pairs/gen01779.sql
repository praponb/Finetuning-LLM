SELECT s.country, s.browser, c.age FROM sessions s JOIN customers c ON c.id = s.customer_id WHERE s.browser = 'Firefox' ORDER BY browser
