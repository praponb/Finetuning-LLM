SELECT s.*, x.session_started_at
FROM customers s
LEFT JOIN LATERAL (
        SELECT c.started_at AS session_started_at
        FROM sessions c
        WHERE c.customer_id = s.id
          AND c.started_at <= s.signup_date
        ORDER BY c.started_at DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE;
