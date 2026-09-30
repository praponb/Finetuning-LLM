SELECT id, opened_date FROM stores WHERE region = 'West' UNION SELECT id, opened_date FROM stores WHERE city IN ('New York', 'Bangkok');
