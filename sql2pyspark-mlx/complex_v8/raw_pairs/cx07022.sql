WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LAST_DAY(claim_date) AS claim_date_month_end
    FROM claims
    WHERE MONTH(reported_at) IN (1, 2)
      AND status = 'IN_REVIEW'
),
unioned AS (
    -- Stack the branches
    SELECT status AS label, reported_at AS event_time
    FROM base_claims
    UNION ALL
    SELECT status, reported_at
    FROM claims
    WHERE policy_id IS NOT NULL
)
SELECT label, COUNT(*) AS n_rows, MAX(event_time) AS latest_event_time
FROM unioned
GROUP BY label
ORDER BY label
