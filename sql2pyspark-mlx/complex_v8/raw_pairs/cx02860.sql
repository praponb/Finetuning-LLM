WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        adjuster_id,
        YEAR(reported_at) AS reported_at_year,
        MAX(approved_amount) AS max_approved_amount,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        MIN(claimed_amount) AS min_claimed_amount,
        COUNT(DISTINCT id) AS distinct_ids
    FROM valid_claims
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM stats
    WHERE n_rows > (SELECT MIN(n_rows) FROM stats)
),
with_children AS (
    -- One row per claim
    SELECT
        s.adjuster_id,
        s.reported_at_year,
        s.max_approved_amount,
        s.distinct_ids,
        c.id AS claim_id,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at,
        c.policy_id AS claim_policy_id
    FROM with_activity s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_children s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.claim_policy_id
              AND c.claimed_amount > 5774.6
        )
      AND s.claim_approved_amount >= 0.0
)
-- Subtotals with GROUPING SETS
SELECT
    adjuster_id,
    claim_id,
    distinct_ids,
    COUNT(*) AS row_count,
    MAX(claim_approved_amount) AS max_claim_approved_amount,
    ROUND(AVG(claim_approved_amount), 2) AS avg_claim_approved_amount
FROM qualified
GROUP BY GROUPING SETS ((adjuster_id, claim_id, distinct_ids), (adjuster_id))
ORDER BY adjuster_id NULLS LAST, claim_id NULLS LAST, distinct_ids NULLS LAST
