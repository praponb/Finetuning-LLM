WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
    FROM claims
    WHERE claim_type = 'flood'
      AND approved_amount > 2272.7
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        least_approved_amount_claimed_amount,
        approved_amount_minus_claimed_amount,
        COUNT(*) OVER (PARTITION BY status, adjuster_id) AS rows_in_status_adjuster_id,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY status, adjuster_id), 2) AS claimed_amount_vs_status_adjuster_id_avg
    FROM valid
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, p.end_date AS policy_end_date, p.status AS policy_status
    FROM qualified s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.end_date IS NOT NULL
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, COUNT(*) AS claim_count, COUNT(*) AS claim_count_2
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_count_2, 0) AS claim_count_2
    FROM enriched s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.reported_at,
        s.least_approved_amount_claimed_amount,
        s.approved_amount_minus_claimed_amount,
        s.first_known_reported_at,
        s.policy_end_date,
        s.claim_count_2,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claim_type
    FROM with_stats s
    JOIN claims c ON c.policy_id = s.policy_id
)
-- Final projection
SELECT
    id,
    reported_at,
    least_approved_amount_claimed_amount,
    first_known_reported_at,
    claim_count_2,
    claim_id,
    claim_type
FROM line_level
WHERE policy_id <= 24
ORDER BY claim_count_2
