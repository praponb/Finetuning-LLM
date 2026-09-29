WITH prepared AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LENGTH(TRIM(team)) AS team_len
    FROM adjusters
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'A%'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_name,
        team,
        level,
        team_len,
        SUM(team_len) OVER (PARTITION BY team_len, level ORDER BY level ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS team_len_sum_1_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY team_len, level ORDER BY level ASC), 3) AS level_percent_rank
    FROM prepared
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.team,
        s.team_len_sum_1_prior,
        s.level_percent_rank,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.approved_amount AS claim_approved_amount
    FROM metrics s
    JOIN claims c ON c.adjuster_id = s.id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        FLOOR(claim_approved_amount / 10000) * 10000 AS claim_approved_amount_bucket
    FROM exploded_detail_claims
)
SELECT
    id,
    team,
    team_len_sum_1_prior,
    level_percent_rank,
    claim_id,
    claim_policy_id,
    claim_approved_amount,
    team_or_unknown,
    claim_approved_amount_bucket,
    CASE WHEN claim_approved_amount >= 17759.3 THEN 'large' WHEN claim_approved_amount >= 2272.7 THEN 'medium' ELSE 'small' END AS claim_approved_amount_tier,
    LEAST(team_len_sum_1_prior, claim_approved_amount) AS least_team_len_sum_1_prior_claim_approved_amount
FROM derived
ORDER BY team DESC NULLS FIRST, id NULLS LAST
