WITH kept_keys AS (
    -- Set difference on keys
    SELECT device, customer_id
    FROM sessions
    EXCEPT ALL
    SELECT device, customer_id
    FROM sessions
    WHERE customer_id IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions s
    INNER JOIN kept_keys k ON s.device = k.device AND s.customer_id = k.customer_id
)
SELECT
    id,
    started_at,
    country
FROM kept_rows
