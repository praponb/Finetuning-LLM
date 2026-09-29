WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label
    FROM claims
    WHERE (status IN ('PAID', 'APPROVED') OR claim_type = 'theft')
),
filtered_2 AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
            WHERE c.reported_at < TIMESTAMP '2025-03-08 14:00:00'
              AND c.claimed_amount < 31752.9
        )
      AND s.claim_date_label IS NOT NULL
)
SELECT
    policy_id,
    adjuster_id,
    GROUPING_ID() AS gid,
    ROUND(SUM(claimed_amount), 2) AS total_claimed_amount,
    COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids
FROM filtered_2
GROUP BY GROUPING SETS ((policy_id, adjuster_id), (policy_id), (adjuster_id))
