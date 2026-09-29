SELECT s.id, s.started_at, s.device, c.email FROM sessions s LEFT JOIN customers c ON s.customer_id = c.id WHERE s.country IN ('UK', 'TH') AND c.signup_date BETWEEN '2024-05-13' AND '2025-06-01';
