WITH remaining_keys AS (
    SELECT device
    FROM sessions
    EXCEPT
    SELECT device
    FROM sessions
    WHERE country IS NULL
),
verified AS (
    SELECT s.*
    FROM sessions s
    INNER JOIN remaining_keys k ON s.device = k.device
)
SELECT device, started_at
FROM verified
ORDER BY started_at DESC
