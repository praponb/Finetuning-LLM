WITH eligible AS (
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.amount > 2400
        )
)
SELECT
    id,
    balance,
    opened_date
FROM eligible
ORDER BY opened_date, balance
