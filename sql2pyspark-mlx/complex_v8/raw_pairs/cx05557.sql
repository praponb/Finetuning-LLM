WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(approved_amount) OVER (PARTITION BY policy_id) AS policy_id_max_approved_amount,
        RANK() OVER (PARTITION BY policy_id, adjuster_id ORDER BY reported_at NULLS LAST, id) AS reported_at_rank
    FROM valid
),
joined AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claim_type_slug,
        s.policy_id_max_approved_amount,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        p.end_date AS policy_end_date
    FROM metrics s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
    WHERE a.level >= 1
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type <> 'medical') FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_claim_type_count
    FROM joined s
),
summary AS (
    -- Group-level totals
    SELECT
        status,
        policy_id,
        YEAR(policy_end_date) AS policy_end_date_year,
        SUM(adjuster_level) FILTER (WHERE adjuster_team = 'Motor') AS adjuster_level_adjuster_team_motor,
        MIN(claim_claim_type_count) AS min_claim_claim_type_count
    FROM profiled
    GROUP BY status, policy_id, YEAR(policy_end_date)
),
with_children AS (
    -- One row per claim
    SELECT
        s.status,
        s.policy_id,
        s.policy_end_date_year,
        s.adjuster_level_adjuster_team_motor,
        s.min_claim_claim_type_count,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.reported_at AS claim_reported_at,
        c.claim_date
    FROM summary s
    JOIN claims c ON c.policy_id = s.policy_id
)
SELECT *
FROM (
    SELECT claim_reported_at, policy_id, status, claim_approved_amount
    FROM with_children
) src
PIVOT (
    MAX(claim_approved_amount)
    FOR status IN ('PAID', 'IN_REVIEW')
)
