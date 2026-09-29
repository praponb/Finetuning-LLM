SELECT s.id, s.customer_id, s.started_at, c.name, c.email, c.city FROM sessions s RIGHT OUTER JOIN customers c ON c.id = s.customer_id WHERE s.country NOT IN ('SG', 'UK') AND NOT c.is_active
