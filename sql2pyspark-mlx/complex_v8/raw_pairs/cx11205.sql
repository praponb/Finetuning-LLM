WITH filtered AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        ABS(level - 3) AS level_distance
    FROM adjusters
    WHERE LENGTH(TRIM(adjuster_name)) >= 7
      AND team IS NOT NULL
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.adjuster_name,
        s.team,
        c.id AS claim_id,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at
    FROM filtered s
    LEFT JOIN claims c ON c.adjuster_id = s.id AND c.reported_at < TIMESTAMP '2025-02-24 07:00:00'
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, SUM(approved_amount) AS total_claim_approved_amount
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_approved_amount
    FROM with_children s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.id
),
team_stats AS (
    -- Avg total_claim_approved_amount per team
    SELECT team, AVG(total_claim_approved_amount) AS avg_total_claim_approved_amount
    FROM profiled
    GROUP BY team
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_total_claim_approved_amount,
        ROUND(s.total_claim_approved_amount - g.avg_total_claim_approved_amount, 2) AS total_claim_approved_amount_minus_avg
    FROM profiled s
    JOIN team_stats g ON g.team = s.team
    WHERE s.total_claim_approved_amount > g.avg_total_claim_approved_amount
),
claim_stats_2 AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, MAX(reported_at) AS last_claim_reported_at
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at
    FROM above_average s
    LEFT JOIN claim_stats_2 c ON c.adjuster_id = s.id
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.adjuster_name,
        s.team,
        s.claim_id,
        s.claim_reported_at,
        s.avg_total_claim_approved_amount,
        s.total_claim_approved_amount_minus_avg,
        s.last_claim_reported_at,
        c.id AS claim_id_2,
        c.claim_type,
        c.policy_id AS claim_policy_id,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status_2
    FROM summarized_rows s
    JOIN claims c ON c.adjuster_id = s.id
)
SELECT *
FROM (
    SELECT team, claim_type, claim_approved_amount
    FROM exploded_detail
) src
PIVOT (
    SUM(claim_approved_amount) AS total, COUNT(claim_approved_amount) AS n
    FOR claim_type IN ('fire', 'delay')
)
