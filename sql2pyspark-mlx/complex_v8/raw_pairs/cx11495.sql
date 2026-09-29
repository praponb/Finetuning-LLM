WITH prepared AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len
    FROM adjusters
    WHERE team IN ('Property', 'Motor')
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        adjuster_name_len,
        LENGTH(TRIM(team)) AS team_len,
        SUBSTRING(TRIM(adjuster_name), 1, 2) AS adjuster_name_prefix,
        LENGTH(TRIM(team)) AS team_len_2
    FROM prepared
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, SUM(approved_amount) AS total_claim_approved_amount
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_approved_amount
    FROM enriched s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_name,
        team,
        level,
        team_len,
        adjuster_name_prefix,
        team_len_2,
        total_claim_approved_amount,
        DENSE_RANK() OVER (PARTITION BY level, team_len_2 ORDER BY level DESC) AS level_dense_rank,
        ROUND(AVG(team_len) OVER (PARTITION BY level, team_len_2 ORDER BY level DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS team_len_avg_6_prior,
        FIRST_VALUE(total_claim_approved_amount) OVER (PARTITION BY level, team_len_2 ORDER BY level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_total_claim_approved_amount
    FROM with_counts
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.level,
        s.team_len,
        s.adjuster_name_prefix,
        s.total_claim_approved_amount,
        s.level_dense_rank,
        s.first_total_claim_approved_amount,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_date,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status
    FROM sequenced s
    JOIN claims c ON c.adjuster_id = s.id AND c.claimed_amount BETWEEN 6745.1 AND 31715.7
)
-- Final projection
SELECT
    id,
    level,
    team_len,
    adjuster_name_prefix,
    total_claim_approved_amount,
    first_total_claim_approved_amount,
    claim_id,
    claim_approved_amount,
    claim_date,
    claim_status
FROM line_level_claims
ORDER BY total_claim_approved_amount, id, claim_id
LIMIT 10
