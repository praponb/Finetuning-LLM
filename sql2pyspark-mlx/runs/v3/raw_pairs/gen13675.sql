SELECT s.started_at, s.browser, s.customer_id, c.segment, c.credit_limit, c.country FROM sessions s INNER JOIN customers c ON s.customer_id = c.id WHERE s.started_at >= '2025-07-22'
