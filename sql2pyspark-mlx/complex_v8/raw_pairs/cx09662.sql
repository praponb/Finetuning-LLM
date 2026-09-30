WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        ROUND(claimed_amount * 1.5, 2) AS claimed_amount_adj,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE adjuster_id <= 3
),
joined_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        p.status AS policy_status
    FROM scoped s
    LEFT JOIN policies p ON p.id = s.policy_id
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MIN(reported_at) AS first_claim_reported_at,
        MIN(claim_date) AS first_claim_date
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_reported_at,
        c.first_claim_date
    FROM joined_policy s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claim_date IS NOT NULL
        )
),
deduped AS (
    -- Keep the earliest row per policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        policy_status,
        first_claim_reported_at,
        first_claim_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY claim_date, id) AS rn
        FROM with_activity
    ) ranked
    WHERE rn = 1
),
with_details_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.policy_status,
        s.first_claim_reported_at,
        s.first_claim_date,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM deduped s
    JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    adjuster_id,
    COALESCE(adjuster_team, 'ALL') AS adjuster_team,
    COUNT(*) AS row_count,
    ROUND(STDDEV(approved_amount), 2) AS stddev_approved_amount
FROM with_details_adjuster
GROUP BY CUBE(adjuster_id, adjuster_team)
