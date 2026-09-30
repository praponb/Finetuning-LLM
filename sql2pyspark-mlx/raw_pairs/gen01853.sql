WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.amount BETWEEN 3100 AND 3200
        )
)
SELECT
    id,
    balance,
    opened_date,
    status
FROM kept
ORDER BY id DESC, status DESC
LIMIT 10;
