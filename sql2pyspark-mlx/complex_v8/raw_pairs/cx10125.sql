WITH prepared_adjusters AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        SUBSTRING(TRIM(team), 1, 3) AS team_prefix,
        CASE WHEN team IN ('Motor', 'Health') THEN 'group_a' WHEN team = 'Property' THEN 'group_b' ELSE 'other' END AS team_group,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown
    FROM adjusters
    WHERE (UPPER(TRIM(adjuster_name)) LIKE 'G%' OR level >= 2)
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM prepared_adjusters s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
              AND c.approved_amount BETWEEN 0.0 AND 33690.0
              AND c.claimed_amount < 40639.7
        )
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(level) OVER w_team_group_team AS team_group_team_max_level,
        ROW_NUMBER() OVER (PARTITION BY team_or_unknown ORDER BY level DESC, id) AS seq_no,
        ROUND(AVG(level) OVER (PARTITION BY team_or_unknown ORDER BY level DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS level_avg_1_prior,
        LAST_VALUE(level) OVER (PARTITION BY team_or_unknown ORDER BY level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_level,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY team_group, team), 0), 4) AS level_share_of_team_group_team
    FROM filtered
    WINDOW w_team_group_team AS (PARTITION BY team_group, team)
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.adjuster_id = s.id) AS last_claim_date,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.adjuster_id = s.id) AS last_claim_date_2
    FROM windowed s
),
claim_stats_2 AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(CASE WHEN claim_type = 'medical' THEN 1 END) AS claim_claim_type_count
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM summarized_rows s
    LEFT JOIN claim_stats_2 c ON c.adjuster_id = s.id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(team_group) OVER w_team_last_claim_date_2_year) AS distinct_team_group_in_team_last_claim_date_2_ye,
        ROW_NUMBER() OVER (PARTITION BY team_or_unknown, last_level ORDER BY last_level, id) AS seq_no_2,
        MIN(level) OVER w_team_last_claim_date_2_year AS team_last_claim_date_2_year_min_level,
        NTILE(4) OVER (PARTITION BY YEAR(last_claim_date) ORDER BY team_group_team_max_level, id) AS team_group_team_max_level_quartile,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(last_claim_date) ORDER BY team_group_team_max_level), 3) AS team_group_team_max_level_cume_dist
    FROM with_stats
    WINDOW w_team_last_claim_date_2_year AS (PARTITION BY team, YEAR(last_claim_date_2))
),
deduped AS (
    -- Keep the earliest row per team_group
    SELECT
        id,
        adjuster_name,
        team,
        level,
        team_prefix,
        team_group,
        team_or_unknown,
        team_group_team_max_level,
        seq_no,
        level_avg_1_prior,
        last_level,
        level_share_of_team_group_team,
        last_claim_date,
        last_claim_date_2,
        claim_claim_type_count,
        distinct_team_group_in_team_last_claim_date_2_ye,
        seq_no_2,
        team_last_claim_date_2_year_min_level,
        team_group_team_max_level_quartile,
        team_group_team_max_level_cume_dist
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team_group ORDER BY team_group_team_max_level_quartile, id) AS rn
        FROM ranked
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    adjuster_name,
    team_prefix,
    team_or_unknown,
    team_group_team_max_level,
    seq_no,
    level_avg_1_prior,
    last_level,
    last_claim_date,
    claim_claim_type_count,
    distinct_team_group_in_team_last_claim_date_2_ye,
    seq_no_2,
    team_group_team_max_level_quartile,
    team_group_team_max_level_cume_dist,
    FLOOR(seq_no / 2) * 2 AS seq_no_bucket,
    DATEDIFF(last_claim_date_2, last_claim_date) AS days_last_claim_date_to_last_claim_date_2
FROM deduped
ORDER BY claim_claim_type_count NULLS LAST, id DESC
