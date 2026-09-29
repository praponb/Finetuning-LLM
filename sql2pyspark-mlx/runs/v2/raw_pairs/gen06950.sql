SELECT c.id, c.email FROM customers c WHERE c.id IN (SELECT customer_id FROM sessions WHERE country IN ('UK', 'DE')) ORDER BY c.id DESC, c.email DESC;
