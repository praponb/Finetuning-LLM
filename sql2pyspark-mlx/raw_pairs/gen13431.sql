SELECT customer_id FROM accounts WHERE opened_date >= '2024-11-01' UNION ALL SELECT customer_id FROM accounts WHERE status IS NULL ORDER BY customer_id DESC
