WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        csat,
        tags,
        ABS(csat - 3) AS csat_distance
    FROM tickets
),
enriched AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.agent_name,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM base s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND UPPER(TRIM(su.agent_name)) LIKE 'K%'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(csat_distance - AVG(csat_distance) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), agent_tier), 2) AS csat_distance_vs_agent_hired_on_month_agent_tier,
        RANK() OVER (PARTITION BY agent_tier ORDER BY csat_distance ASC) AS csat_distance_rank,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), agent_tier) AS rows_in_agent_hired_on_month_agent_tier,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_6_prior,
        SIZE(COLLECT_SET(agent_id) OVER w_agent_hired_on_month_agent_t) AS distinct_agent_id_in_agent_hired_on_month_agent_,
        RANK() OVER (PARTITION BY agent_id ORDER BY opened_at DESC) AS opened_at_rank,
        ROUND(AVG(csat) OVER (PARTITION BY agent_tier ORDER BY csat_distance ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS csat_avg_7,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS csat_avg_2,
        ROUND(csat_distance / NULLIF(SUM(csat_distance) OVER w_agent_hired_on_month_agent_t, 0), 4) AS csat_distance_share_of_agent_hired_on_month_agen
    FROM enriched
    WINDOW w_agent_hired_on_month_agent_t AS (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), agent_tier)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        agent_id,
        opened_at,
        csat,
        tags,
        csat_distance,
        agent_name,
        agent_tier,
        agent_hired_on,
        csat_distance_vs_agent_hired_on_month_agent_tier,
        csat_distance_rank,
        first_known_opened_at,
        rows_in_agent_hired_on_month_agent_tier,
        csat_avg_6_prior,
        distinct_agent_id_in_agent_hired_on_month_agent_,
        csat_avg_7,
        csat_avg_2,
        csat_distance_share_of_agent_hired_on_month_agen
    FROM ranked
    WHERE opened_at_rank <= 3
)
SELECT
    id,
    agent_id,
    opened_at,
    csat,
    tags,
    agent_name,
    agent_hired_on,
    csat_distance_vs_agent_hired_on_month_agent_tier,
    csat_distance_rank,
    first_known_opened_at,
    csat_avg_6_prior,
    csat_avg_7,
    csat_avg_2,
    csat_distance_share_of_agent_hired_on_month_agen,
    CASE WHEN agent_tier IN ('T1', 'T2') THEN 'group_a' WHEN agent_tier = 'T3' THEN 'group_b' ELSE 'other' END AS agent_tier_group,
    ABS(csat_avg_2 - 2.6) AS csat_avg_2_distance
FROM first_rows
