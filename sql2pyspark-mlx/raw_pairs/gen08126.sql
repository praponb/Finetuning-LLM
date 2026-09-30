SELECT
    s.id,
    s.email,
    s.segment,
    s.signup_date,
    s.is_active,
    s.credit_limit,
    x.status
FROM customers s
JOIN LATERAL (
        SELECT c.status
        FROM accounts c
        WHERE c.customer_id = s.id
          AND c.opened_date <= s.signup_date
        ORDER BY c.opened_date DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE
