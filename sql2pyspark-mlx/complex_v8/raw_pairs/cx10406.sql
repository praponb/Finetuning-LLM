WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        csat,
        tags
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 7 AND 11
      AND csat >= 3
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY agent_id ORDER BY opened_at NULLS LAST, id) AS opened_at_rank,
        NTH_VALUE(csat, 2) OVER (PARTITION BY agent_id ORDER BY opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        SUM(csat) OVER (PARTITION BY agent_id ORDER BY opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat
    FROM valid_tickets
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.csat,
        s.tags,
        s.opened_at_rank,
        s.second_csat,
        su.team AS agent_team,
        su.agent_name
    FROM ranked s
    JOIN support_agents su ON su.id = s.agent_id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        second_csat,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        ROUND(100 * COUNT(CASE WHEN agent_team = 'Onboarding' THEN 1 END) / COUNT(*), 1) AS pct_agent_team_onboarding,
        SUM(csat) FILTER (WHERE LOWER(agent_name) LIKE '%t %') AS csat_lower_agent_name_like_t,
        MAX(opened_at) AS last_opened_at,
        MIN(opened_at_rank) AS min_opened_at_rank,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_parent
    WHERE ARRAY_CONTAINS(tags, 'login')
    GROUP BY second_csat, DATE_TRUNC('MONTH', opened_at)
),
annotated AS (
    -- Derive helper columns
    SELECT
        second_csat,
        opened_at_month,
        csat_lower_agent_name_like_t,
        last_opened_at,
        min_opened_at_rank,
        ROUND(min_opened_at_rank / NULLIF(second_csat, 0), 3) AS min_opened_at_rank_per_second_csat,
        COALESCE(min_opened_at_rank, 0) AS min_opened_at_rank_filled
    FROM stats
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        TO_DATE(opened_at_month) AS opened_at_month_date,
        DATE_TRUNC('YEAR', last_opened_at) AS last_opened_at_year
    FROM annotated
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(last_opened_at_year) - UNIX_TIMESTAMP(last_opened_at)) / 3600, 1) AS hours_last_opened_at_to_last_opened_at_year,
        CASE WHEN min_opened_at_rank >= 9 THEN 'large' WHEN min_opened_at_rank >= 3 THEN 'medium' ELSE 'small' END AS min_opened_at_rank_tier,
        COALESCE(min_opened_at_rank, 0) AS min_opened_at_rank_filled_2
    FROM tagged
)
SELECT
    second_csat,
    csat_lower_agent_name_like_t,
    min_opened_at_rank,
    min_opened_at_rank_filled,
    hours_last_opened_at_to_last_opened_at_year,
    min_opened_at_rank_tier,
    min_opened_at_rank_filled_2,
    CASE WHEN min_opened_at_rank_tier IN ('medium', 'small') THEN 'group_a' WHEN min_opened_at_rank_tier = 'large' THEN 'group_b' ELSE 'other' END AS min_opened_at_rank_tier_group,
    DATE_FORMAT(opened_at_month, 'yyyy-MM-dd HH:00') AS opened_at_month_label
FROM labeled
WHERE second_csat <= 4
ORDER BY min_opened_at_rank, second_csat DESC NULLS FIRST
