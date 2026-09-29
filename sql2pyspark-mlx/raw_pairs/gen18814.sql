SELECT
    s.*,
    latest.balance,
    latest.opened_date
FROM customers s
JOIN LATERAL (
        SELECT c.balance, c.opened_date
        FROM accounts c
        WHERE c.customer_id = s.id
        ORDER BY c.balance DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
