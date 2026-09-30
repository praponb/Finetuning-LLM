SELECT s.started_at, c.signup_date, c.city FROM sessions s RIGHT JOIN customers c ON s.customer_id = c.id WHERE c.segment NOT IN ('SMB', 'Corporate') ORDER BY signup_date;
