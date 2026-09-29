WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        ROUND(premium * 0.9, 2) AS premium_adj
    FROM policies
    WHERE status = 'EXPIRED'
      AND holder_id IS NOT NULL
),
stacked AS (
    -- Stack the branches
    SELECT product AS label, id, start_date AS event_date
    FROM prepared_policys
    UNION
    SELECT claim_type AS label, id, claim_date AS event_date
    FROM claims
)
SELECT label, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(event_date) AS latest_event_date
FROM stacked
GROUP BY label
ORDER BY label
