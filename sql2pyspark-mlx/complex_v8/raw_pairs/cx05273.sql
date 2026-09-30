WITH recent AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LOWER(TRIM(adjuster_name)) AS adjuster_name_clean,
        ABS(level - 3) AS level_distance,
        CASE WHEN team IN ('Health', 'Property') THEN 'group_a' WHEN team = 'Travel' THEN 'group_b' ELSE 'other' END AS team_group
    FROM adjusters
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'A%'
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type = 'fire') FROM claims c WHERE c.adjuster_id = s.id) AS claim_claim_type_count,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.adjuster_id = s.id) AS avg_claim_approved_amount
    FROM recent s
),
latest AS (
    -- Keep the latest row per team, team_group
    SELECT
        id,
        adjuster_name,
        team,
        level,
        adjuster_name_clean,
        level_distance,
        team_group,
        claim_claim_type_count,
        avg_claim_approved_amount
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team, team_group ORDER BY level_distance DESC, id DESC) AS rn
        FROM profiled
    ) ranked
    WHERE rn = 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(claim_claim_type_count) OVER w_claim_claim_type_count AS claim_claim_type_count_sum_claim_claim_type_coun,
        ROUND(AVG(avg_claim_approved_amount) OVER (PARTITION BY team_group, level ORDER BY level_distance DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS avg_claim_approved_amount_avg_3_prior,
        COUNT(*) OVER (PARTITION BY claim_claim_type_count) AS rows_in_claim_claim_type_count,
        FIRST_VALUE(avg_claim_approved_amount) OVER (PARTITION BY team_group, level ORDER BY level_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_avg_claim_approved_amount
    FROM latest
    WINDOW w_claim_claim_type_count AS (PARTITION BY claim_claim_type_count)
)
SELECT
    id,
    adjuster_name,
    team,
    level,
    adjuster_name_clean,
    team_group,
    avg_claim_approved_amount,
    claim_claim_type_count_sum_claim_claim_type_coun,
    rows_in_claim_claim_type_count,
    first_avg_claim_approved_amount,
    CASE WHEN level >= 3 THEN 'HIGH' WHEN level >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS level_tier,
    ABS(claim_claim_type_count - 2) AS claim_claim_type_count_distance
FROM with_trends
