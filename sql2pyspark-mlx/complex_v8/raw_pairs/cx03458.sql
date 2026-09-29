WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE MONTH(reported_at) IN (2, 3)
      AND status IN ('PAID', 'REJECTED', 'APPROVED')
),
combined AS (
    -- Stack the branches
    SELECT id AS qty, claim_date AS event_date, policy_id AS entity_id
    FROM claims
    WHERE approved_amount <= 15477.7
      AND status IN ('PAID', 'REJECTED')
    UNION ALL
    SELECT id, start_date, holder_id
    FROM policies
    WHERE start_date IS NOT NULL
      AND premium < 3465.4
    UNION ALL
    SELECT id, end_date, holder_id
    FROM policies
)
SELECT qty, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id, MAX(event_date) AS latest_event_date
FROM combined
GROUP BY qty
ORDER BY qty
