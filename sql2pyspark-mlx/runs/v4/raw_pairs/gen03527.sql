SELECT s.*, latest.transaction_amount
FROM accounts s
LEFT JOIN LATERAL (
        SELECT c.amount AS transaction_amount
        FROM transactions c
        WHERE c.account_id = s.id
          AND c.txn_time <= s.opened_date
        ORDER BY c.txn_time DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
