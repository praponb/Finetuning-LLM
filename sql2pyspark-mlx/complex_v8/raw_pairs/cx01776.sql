WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        UPPER(TRIM(claim_type)) AS claim_type_clean,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 7 AND 9
      AND claimed_amount <= 43500.4
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, COUNT(*) AS claim_count
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM base_claims s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
enriched_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.claim_type_clean,
        s.approved_amount_bucket,
        s.claimed_amount_plus_approved_amount,
        s.claim_count,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM profiled s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team IN ('Health', 'Travel')
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM enriched_adjuster s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claim_type = 'delay'
              AND c.claimed_amount >= 5313.8
        )
      AND s.claim_count <= 6
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, adjuster_level, claim_count, approved_amount_bucket
    FROM screened
) src
UNPIVOT (
    value FOR measure IN (adjuster_level, claim_count, approved_amount_bucket)
) unp
ORDER BY id, measure
