WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE status IS NOT NULL
),
with_parent_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        p.end_date AS policy_end_date,
        p.start_date AS policy_start_date
    FROM scoped_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id AND UPPER(TRIM(a.adjuster_name)) LIKE 'E%'
    JOIN policies p ON p.id = s.policy_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        claim_date,
        reported_at,
        adjuster_team,
        adjuster_level,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER (PARTITION BY adjuster_team, DATE_TRUNC('MONTH', policy_start_date)), 0), 4) AS adjuster_level_share_of_adjuster_team_policy_sta,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY adjuster_team ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        NTH_VALUE(adjuster_level, 2) OVER (PARTITION BY adjuster_team ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level,
        ROUND(AVG(adjuster_level) OVER (ORDER BY claimed_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_4,
        ROUND(CUME_DIST() OVER (ORDER BY claimed_amount), 3) AS claimed_amount_cume_dist,
        FIRST_VALUE(policy_start_date) IGNORE NULLS OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_policy_start_date,
        LAST_VALUE(adjuster_level) IGNORE NULLS OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_adjuster_level,
        MAX(adjuster_level) OVER (PARTITION BY adjuster_team ORDER BY reported_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS adjuster_level_max_3_prior,
        ROUND(AVG(claimed_amount) OVER w_adjuster_team_policy_start_d, 2) AS adjuster_team_policy_start_date_month_avg_claime,
        LEAD(claim_date, 2) OVER (ORDER BY claimed_amount, id) AS next2_claim_date,
        NTILE(3) OVER (PARTITION BY adjuster_team ORDER BY reported_at, id) AS reported_at_tercile
    FROM with_parent_adjuster
    WINDOW w_adjuster_team_policy_start_d AS (PARTITION BY adjuster_team, DATE_TRUNC('MONTH', policy_start_date))
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(adjuster_level_avg_4 * 1.1, 2) AS adjuster_level_avg_4_adj,
        SUBSTRING(TRIM(adjuster_team), 1, 4) AS adjuster_team_prefix
    FROM windowed
    WHERE second_adjuster_level <= 3
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        reported_at,
        adjuster_team,
        adjuster_level,
        adjuster_level_share_of_adjuster_team_policy_sta,
        first_known_claimed_amount,
        second_adjuster_level,
        adjuster_level_avg_4,
        last_known_adjuster_level,
        adjuster_level_max_3_prior,
        next2_claim_date,
        adjuster_level_avg_4_adj,
        ROUND(adjuster_level_avg_4 / NULLIF(SUM(adjuster_level_avg_4) OVER (PARTITION BY adjuster_id), 0), 4) AS adjuster_level_avg_4_share_of_adjuster_id,
        ROUND(first_known_claimed_amount - AVG(first_known_claimed_amount) OVER (PARTITION BY adjuster_id), 2) AS first_known_claimed_amount_vs_adjuster_id_avg,
        FIRST_VALUE(next2_claim_date) OVER (PARTITION BY MONTH(claim_date) ORDER BY adjuster_level_share_of_adjuster_team_policy_sta, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_next2_claim_date,
        SIZE(COLLECT_SET(adjuster_team) OVER w_adjuster_id) AS distinct_adjuster_team_in_adjuster_id,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (ORDER BY reported_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        ROUND(AVG(last_known_adjuster_level) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_last_known_adjuster_level,
        NTH_VALUE(second_adjuster_level, 2) OVER (PARTITION BY MONTH(claim_date) ORDER BY adjuster_level_share_of_adjuster_team_policy_sta, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_adjuster_level
    FROM tagged
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id)
)
-- Final projection
SELECT
    id,
    reported_at,
    adjuster_level,
    adjuster_level_share_of_adjuster_team_policy_sta,
    second_adjuster_level,
    adjuster_level_avg_4,
    last_known_adjuster_level,
    next2_claim_date,
    adjuster_level_avg_4_adj,
    adjuster_level_avg_4_share_of_adjuster_id,
    first_next2_claim_date,
    distinct_adjuster_team_in_adjuster_id,
    first_known_claim_date,
    adjuster_id_avg_last_known_adjuster_level,
    second_second_adjuster_level
FROM ranked
WHERE adjuster_level_max_3_prior < 12
