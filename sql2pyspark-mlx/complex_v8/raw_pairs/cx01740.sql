WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        CONCAT_WS(': ', tier, team) AS tier_team_label,
        LOWER(REGEXP_REPLACE(TRIM(tier), ' ', '_')) AS tier_slug
    FROM support_agents
    WHERE tier IN ('T3', 'T2')
      AND team = 'Onboarding'
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        COUNT(CASE WHEN channel NOT IN ('email', 'phone') THEN 1 END) AS ticket_channel_count,
        COUNT(*) AS ticket_count_2
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        COALESCE(t.ticket_channel_count, 0) AS ticket_channel_count,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2
    FROM base s
    LEFT JOIN ticket_stats t ON t.agent_id = s.id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(ticket_count_2) OVER (PARTITION BY MONTH(hired_on)), 2) AS hired_on_month_avg_ticket_count_2,
        ROW_NUMBER() OVER (PARTITION BY ticket_count ORDER BY ticket_count ASC NULLS LAST, id) AS rn,
        MAX(ticket_count) OVER (PARTITION BY ticket_count ORDER BY ticket_count ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ticket_count_max_6_prior,
        MIN(ticket_channel_count) OVER (PARTITION BY MONTH(hired_on)) AS hired_on_month_min_ticket_channel_count,
        COUNT(*) OVER w_hired_on_month AS rows_in_hired_on_month
    FROM summarized_rows
    WINDOW w_hired_on_month AS (PARTITION BY MONTH(hired_on))
),
summarized_rows_2 AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.requester_id) FROM tickets t WHERE t.agent_id = s.id) AS max_ticket_requester_id,
        (SELECT ROUND(AVG(t.csat), 2) FROM tickets t WHERE t.agent_id = s.id) AS avg_ticket_csat
    FROM with_trends s
)
SELECT id, measure, amount
FROM (
    SELECT id, rows_in_hired_on_month, rn, ticket_count_2, hired_on_month_min_ticket_channel_count
    FROM summarized_rows_2
) src
UNPIVOT (
    amount FOR measure IN (rows_in_hired_on_month AS m_rows_in_hired_on_month, rn AS rn_v, ticket_count_2 AS ticket_count_2_v, hired_on_month_min_ticket_channel_count AS m_hired_on_month_min_ticket_channel_count)
) unp
