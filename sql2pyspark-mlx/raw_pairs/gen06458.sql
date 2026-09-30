WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        status,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('WEEK', opened_date), customer_id ORDER BY opened_date, id) AS event_seq,
        LAG(balance, 1) OVER (PARTITION BY account_type ORDER BY opened_date, id) AS prev_balance
    FROM accounts
)
SELECT
    id,
    balance,
    status,
    event_seq,
    prev_balance
FROM with_windows;
