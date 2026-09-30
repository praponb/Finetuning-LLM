WITH stacked AS (
    -- Stack the branches
    SELECT id AS key_id, claim_date AS day, claimed_amount AS value
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-31 09:00:00'
    UNION
    SELECT holder_id, start_date, premium
    FROM policies
)
SELECT key_id, COUNT(*) AS n_rows, SUM(value) AS total_value, MAX(day) AS latest_day
FROM stacked
GROUP BY key_id
ORDER BY key_id
