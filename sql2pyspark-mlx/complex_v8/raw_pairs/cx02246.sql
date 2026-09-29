WITH valid_claims AS (
    -- Filter claims
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
    WHERE policy_id IS NOT NULL
      AND status IS NOT NULL
),
claim_type_stats AS (
    -- Avg approved_amount per claim_type
    SELECT claim_type, AVG(approved_amount) AS avg_approved_amount
    FROM valid_claims
    GROUP BY claim_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount,
        ROUND(s.approved_amount - g.avg_approved_amount, 2) AS approved_amount_minus_avg
    FROM valid_claims s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.approved_amount > g.avg_approved_amount
),
summary AS (
    -- Group-level totals
    SELECT
        policy_id,
        adjuster_id,
        claim_type,
        ROUND(SUM(avg_approved_amount) / NULLIF(SUM(claimed_amount), 0), 3) AS avg_approved_amount_to_claimed_amount_ratio,
        SUM(approved_amount_minus_avg) AS total_approved_amount_minus_avg,
        SUM(CASE WHEN status <> 'OPEN' THEN avg_approved_amount ELSE 0 END) AS avg_approved_amount_status_open
    FROM outliers
    GROUP BY policy_id, adjuster_id, claim_type
    HAVING MAX(approved_amount) > 4234.9
)
SELECT
    adjuster_id,
    claim_type,
    SUM(CASE WHEN claim_type IN ('collision', 'theft', 'flood') THEN 1 ELSE 0 END) AS n_claim_type_in_collision_thef
FROM summary
GROUP BY GROUPING SETS ((adjuster_id, claim_type), (adjuster_id), (claim_type), ())
