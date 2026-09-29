WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT_WS('|', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE (agent_id IS NOT NULL OR SIZE(tags) >= 1)
),
enriched AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM scoped_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.csat,
        s.tags,
        s.channel_priority_label,
        s.agent_hired_on,
        s.agent_team,
        s.agent_tier,
        su.team AS agent_team_2,
        su.tier AS agent_tier_2,
        su.hired_on AS agent_hired_on_2
    FROM enriched s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        channel,
        agent_tier,
        DATE_TRUNC('MONTH', agent_hired_on) AS agent_hired_on_month,
        SUM(CASE WHEN agent_team <> 'Onboarding' THEN 1 ELSE 0 END) AS n_agent_team_onboarding,
        ROUND(AVG(csat), 2) AS avg_csat,
        COUNT(*) AS cnt,
        COUNT(DISTINCT agent_tier) AS distinct_agent_tiers,
        SUM(csat) AS total_csat
    FROM with_details
    WHERE agent_team = 'Tech'
    GROUP BY channel, agent_tier, DATE_TRUNC('MONTH', agent_hired_on)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE avg_csat > (SELECT AVG(avg_csat) FROM rollup_base)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY cnt, agent_tier) AS rows_in_cnt_agent_tier,
        DENSE_RANK() OVER w_n_agent_team_onboarding_cnt AS avg_csat_dense_rank
    FROM filtered
    WINDOW w_n_agent_team_onboarding_cnt AS (PARTITION BY n_agent_team_onboarding, cnt ORDER BY avg_csat DESC)
)
SELECT
    channel,
    agent_hired_on_month,
    n_agent_team_onboarding,
    cnt,
    distinct_agent_tiers,
    total_csat,
    rows_in_cnt_agent_tier,
    avg_csat_dense_rank,
    SUBSTRING(TRIM(agent_tier), 1, 4) AS agent_tier_prefix
FROM metrics
ORDER BY n_agent_team_onboarding DESC
