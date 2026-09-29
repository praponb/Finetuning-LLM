WITH valid AS (
    -- Scope the claims rows
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
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label,
        QUARTER(claim_date) AS claim_date_quarter
    FROM claims
    WHERE policy_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(claimed_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        ROUND(claim_date_quarter - AVG(claim_date_quarter) OVER (PARTITION BY adjuster_id), 2) AS claim_date_quarter_vs_adjuster_id_avg,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        MAX(approved_amount) OVER (PARTITION BY status, YEAR(claim_date) ORDER BY reported_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS approved_amount_max_7,
        COUNT(*) OVER (PARTITION BY adjuster_id) AS rows_in_adjuster_id
    FROM valid
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
      AND s.adjuster_id <= 7
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_date
    FROM without_matches s
),
with_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claim_date_label,
        s.claim_date_quarter,
        s.running_claimed_amount,
        s.first_known_claimed_amount,
        s.approved_amount_max_7,
        s.rows_in_adjuster_id,
        s.last_claim_date,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name,
        p.premium AS policy_premium
    FROM with_counts s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    approved_amount,
    status,
    reported_at,
    claim_date_label,
    claim_date_quarter,
    running_claimed_amount,
    approved_amount_max_7,
    rows_in_adjuster_id,
    last_claim_date,
    adjuster_level,
    adjuster_name,
    CONCAT(adjuster_team, '#', CAST(policy_id AS STRING)) AS adjuster_team_ref
FROM with_details_adjuster
ORDER BY claim_date_quarter DESC, id
LIMIT 10
