SELECT c.id, c.segment FROM customers c WHERE c.signup_date < '2023-08-22' AND c.id IN (SELECT customer_id FROM sessions WHERE country IN ('TH', 'JP', 'SG')) ORDER BY id
