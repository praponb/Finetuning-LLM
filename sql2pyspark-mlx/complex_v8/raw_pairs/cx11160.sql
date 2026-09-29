WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd HH:00') AS closed_at_label,
        CONCAT_WS(' / ', priority, channel) AS priority_channel_label
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-24 08:00:00'
      AND agent_id IS NOT NULL
),
enriched_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.closed_at_label,
        s.priority_channel_label,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.agent_name
    FROM base_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        UPPER(TRIM(agent_name)) AS agent_name_clean,
        INITCAP(TRIM(channel)) AS channel_clean,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat
    FROM enriched_agent
    WHERE agent_hired_on < DATE '2025-02-27'
),
combined_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.closed_at_label,
        s.agent_hired_on,
        s.agent_team,
        s.greatest_requester_id_csat,
        su.hired_on AS agent_hired_on_2,
        su.tier AS agent_tier,
        su.team AS agent_team_2
    FROM derived s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team <> 'Tech'
),
per_group AS (
    -- Summarise per group
    SELECT
        agent_team_2,
        YEAR(agent_hired_on_2) AS agent_hired_on_2_year,
        COUNT(DISTINCT agent_team_2) AS distinct_agent_team_2s,
        MAX(agent_hired_on) AS last_agent_hired_on,
        COUNT(*) FILTER (WHERE agent_team_2 IN ('VIP', 'Tech')) AS n_agent_team_2_in_vip_tech,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        ROUND(100 * COUNT(CASE WHEN agent_team_2 = 'Billing' THEN 1 END) / COUNT(*), 1) AS pct_agent_team_2_billing
    FROM combined_details
    GROUP BY agent_team_2, YEAR(agent_hired_on_2)
    HAVING MAX(greatest_requester_id_csat) > 6
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        agent_hired_on_2_year - LAG(agent_hired_on_2_year) OVER (PARTITION BY agent_hired_on_2_year, n_agent_team_2_in_vip_tech ORDER BY distinct_agent_team_2s, agent_team_2, agent_hired_on_2_year) AS agent_hired_on_2_year_change,
        ROUND(pct_agent_team_2_billing - AVG(pct_agent_team_2_billing) OVER (PARTITION BY agent_team_2), 2) AS pct_agent_team_2_billing_vs_agent_team_2_avg,
        ROUND(n_agent_team_2_in_vip_tech / NULLIF(SUM(n_agent_team_2_in_vip_tech) OVER w_agent_team_2, 0), 4) AS n_agent_team_2_in_vip_tech_share_of_agent_team_2,
        NTILE(5) OVER w_agent_hired_on_2_year_n_agen AS distinct_agent_team_2s_quintile
    FROM per_group
    WINDOW w_agent_team_2 AS (PARTITION BY agent_team_2), w_agent_hired_on_2_year_n_agen AS (PARTITION BY agent_hired_on_2_year, n_agent_team_2_in_vip_tech ORDER BY distinct_agent_team_2s, agent_team_2, agent_hired_on_2_year)
),
derived_2 AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(last_agent_hired_on, 'MMM yyyy') AS last_agent_hired_on_label,
        DATEDIFF(DATE '2026-01-01', last_agent_hired_on) AS days_since_last_agent_hired_on
    FROM ranked
)
-- Final projection
SELECT
    agent_team_2,
    distinct_agent_team_2s,
    last_agent_hired_on,
    n_agent_team_2_in_vip_tech,
    distinct_agent_ids,
    pct_agent_team_2_billing,
    pct_agent_team_2_billing_vs_agent_team_2_avg,
    n_agent_team_2_in_vip_tech_share_of_agent_team_2,
    distinct_agent_team_2s_quintile
FROM derived_2
ORDER BY pct_agent_team_2_billing, distinct_agent_ids
