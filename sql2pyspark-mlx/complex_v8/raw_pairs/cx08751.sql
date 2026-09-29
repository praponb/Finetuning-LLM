WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE claim_type IS NOT NULL
      AND approved_amount <= 14595.6
),
deduped AS (
    -- Keep the latest row per policy_id, adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        least_approved_amount_claimed_amount
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, adjuster_id ORDER BY least_approved_amount_claimed_amount DESC, id DESC) AS row_num
        FROM cleaned_claims
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with CUBE
SELECT
    COALESCE(status, 'ALL') AS status,
    adjuster_id,
    policy_id,
    COUNT(*) AS n_rows,
    MAX(reported_at) AS last_reported_at
FROM deduped
GROUP BY CUBE(status, adjuster_id, policy_id)
ORDER BY status NULLS LAST, adjuster_id NULLS LAST, policy_id NULLS LAST
