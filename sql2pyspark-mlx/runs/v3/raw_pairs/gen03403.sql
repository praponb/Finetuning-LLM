SELECT *, TO_DATE(started_at) started_at_date FROM sessions WHERE HOUR(started_at) BETWEEN 11 AND 14 AND customer_id IN (3, 15);
