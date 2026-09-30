WITH scoped_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        COALESCE(level, 0) AS level_filled
    FROM adjusters
    WHERE adjuster_name IS NOT NULL
      AND level BETWEEN 1 AND 3
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        team,
        level,
        team_or_unknown,
        level_filled,
        COALESCE(level_filled, 0) AS level_filled_filled,
        LENGTH(TRIM(team)) AS team_len,
        INITCAP(TRIM(team)) AS team_clean
    FROM scoped_adjusters
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(*) AS claim_count,
        MIN(claim_date) AS first_claim_date,
        MIN(claimed_amount) AS min_claim_claimed_amount
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.first_claim_date,
        c.min_claim_claimed_amount
    FROM tagged s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        team,
        level,
        team_or_unknown,
        level_filled,
        level_filled_filled,
        team_len,
        team_clean,
        claim_count,
        first_claim_date,
        min_claim_claimed_amount,
        SIZE(COLLECT_SET(team) OVER w_level_filled) AS distinct_team_in_level_filled,
        ROW_NUMBER() OVER w_level_filled_team_by_level_f AS seq_no,
        MAX(claim_count) OVER (PARTITION BY level_filled) AS level_filled_max_claim_count,
        ROUND(AVG(claim_count) OVER (PARTITION BY YEAR(first_claim_date) ORDER BY level_filled, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS claim_count_avg_2
    FROM with_stats
    WINDOW w_level_filled AS (PARTITION BY level_filled), w_level_filled_team_by_level_f AS (PARTITION BY level_filled, team ORDER BY level_filled DESC, id)
)
SELECT
    id,
    team,
    level,
    team_or_unknown,
    level_filled,
    level_filled_filled,
    team_clean,
    min_claim_claimed_amount,
    distinct_team_in_level_filled,
    seq_no,
    level_filled_max_claim_count,
    claim_count_avg_2
FROM metrics
