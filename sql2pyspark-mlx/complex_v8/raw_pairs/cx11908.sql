WITH filtered_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        DATE_ADD(hired_on, 30) AS hired_on_plus_30d
    FROM support_agents
    WHERE LOWER(agent_name) LIKE '%at%'
      AND team = 'Billing'
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        tier,
        hired_on_plus_30d,
        CONCAT_WS('|', agent_name, tier) AS agent_name_tier_label
    FROM filtered_agents
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        tier,
        agent_name_tier_label,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown,
        LAST_DAY(hired_on_plus_30d) AS hired_on_plus_30d_month_end
    FROM tagged
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.channel IN ('email', 'chat')) FROM tickets t WHERE t.agent_id = s.id) AS ticket_channel_count
    FROM labeled s
),
ticket_stats_2 AS (
    -- Per-agent tickets metrics
    SELECT agent_id, MAX(closed_at) AS last_ticket_closed_at
    FROM tickets
    GROUP BY agent_id
),
with_stats_2 AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.last_ticket_closed_at
    FROM with_stats s
    LEFT JOIN ticket_stats_2 t ON t.agent_id = s.id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        tier,
        agent_name_tier_label,
        tier_or_unknown,
        hired_on_plus_30d_month_end,
        ticket_channel_count,
        last_ticket_closed_at,
        ROW_NUMBER() OVER (PARTITION BY tier_or_unknown, MONTH(last_ticket_closed_at) ORDER BY hired_on_plus_30d_month_end NULLS LAST, id) AS seq_no,
        LAG(ticket_channel_count) OVER w_tier_or_unknown_last_ticket AS prev_ticket_channel_count,
        LAST_VALUE(last_ticket_closed_at) IGNORE NULLS OVER (PARTITION BY tier_or_unknown, MONTH(last_ticket_closed_at) ORDER BY hired_on_plus_30d_month_end NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_ticket_closed_at,
        DENSE_RANK() OVER w_tier_or_unknown_last_ticket_3 AS hired_on_plus_30d_month_end_dense_rank,
        NTH_VALUE(last_ticket_closed_at, 2) OVER (PARTITION BY tier_or_unknown, MONTH(last_ticket_closed_at) ORDER BY hired_on_plus_30d_month_end NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_ticket_closed_at
    FROM with_stats_2
    WINDOW w_tier_or_unknown_last_ticket AS (PARTITION BY tier_or_unknown, MONTH(last_ticket_closed_at) ORDER BY hired_on_plus_30d_month_end NULLS LAST, id), w_tier_or_unknown_last_ticket_3 AS (PARTITION BY tier_or_unknown, MONTH(last_ticket_closed_at) ORDER BY hired_on_plus_30d_month_end NULLS LAST)
)
SELECT
    COALESCE(tier_or_unknown, 'ALL') AS tier_or_unknown,
    ticket_channel_count,
    hired_on_plus_30d_month_end_dense_rank,
    SUM(CASE WHEN tier = 'T1' THEN hired_on_plus_30d_month_end_dense_rank ELSE 0 END) AS hired_on_plus_30d_month_end_dense_rank_tier_t1
FROM with_windows
GROUP BY GROUPING SETS ((tier_or_unknown, ticket_channel_count, hired_on_plus_30d_month_end_dense_rank), (tier_or_unknown), (ticket_channel_count))
ORDER BY tier_or_unknown NULLS LAST, ticket_channel_count NULLS LAST, hired_on_plus_30d_month_end_dense_rank NULLS LAST
