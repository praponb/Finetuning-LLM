SELECT
    s.id,
    s.balance,
    s.opened_date,
    s.status,
    top1.transaction_amount
FROM accounts s
LEFT JOIN LATERAL (
        SELECT c.amount AS transaction_amount
        FROM transactions c
        WHERE c.account_id = s.id
        ORDER BY c.amount DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE
