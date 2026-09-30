WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM tickets
),
enriched_agent AS (
    -- Join support_agents
    SELECT
        s.*,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM cleaned_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team = 'VIP'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(3) OVER w_agent_hired_on_year_by_reque AS requester_id_tercile,
        COUNT(*) OVER (PARTITION BY MONTH(closed_at), agent_id) AS rows_in_closed_at_month_agent_id,
        DENSE_RANK() OVER (PARTITION BY YEAR(agent_hired_on) ORDER BY requester_id) AS requester_id_dense_rank,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY YEAR(agent_hired_on) ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        LAG(opened_at) OVER (PARTITION BY YEAR(agent_hired_on) ORDER BY requester_id, id) AS prev_opened_at,
        ROW_NUMBER() OVER w_agent_team_channel_by_agent AS agent_hired_on_seq,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_team, channel ORDER BY agent_hired_on DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_2,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY MONTH(closed_at), agent_id), 0), 4) AS requester_id_share_of_closed_at_month_agent_id,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY MONTH(closed_at), agent_id)) AS distinct_channel_in_closed_at_month_agent_id,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_team, channel ORDER BY agent_hired_on DESC), 3) AS agent_hired_on_cume_dist,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY MONTH(closed_at), agent_id), 2) AS requester_id_vs_closed_at_month_agent_id_avg
    FROM enriched_agent
    WINDOW w_agent_hired_on_year_by_reque AS (PARTITION BY YEAR(agent_hired_on) ORDER BY requester_id, id), w_agent_team_channel_by_agent AS (PARTITION BY agent_team, channel ORDER BY agent_hired_on DESC, id)
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM ranked
    WHERE requester_id_dense_rank <= 3
),
summary AS (
    -- Group-level totals
    SELECT
        agent_hired_on_seq,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        COUNT(DISTINCT agent_team) AS distinct_agent_teams,
        COUNT(*) AS cnt
    FROM top_rows
    WHERE distinct_channel_in_closed_at_month_agent_id > 2
    GROUP BY 1, 2
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(agent_hired_on_seq) OVER (PARTITION BY cnt, agent_hired_on_seq ORDER BY distinct_agent_teams DESC, agent_hired_on_seq, opened_at_month ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS agent_hired_on_seq_avg_4,
        LEAD(agent_hired_on_seq, 2) OVER (PARTITION BY cnt, agent_hired_on_seq ORDER BY distinct_agent_teams DESC, agent_hired_on_seq, opened_at_month) AS next2_agent_hired_on_seq
    FROM summary
)
SELECT
    agent_hired_on_seq,
    distinct_agent_teams,
    cnt,
    agent_hired_on_seq_avg_4,
    next2_agent_hired_on_seq,
    DATEDIFF(DATE '2026-01-01', opened_at_month) AS days_since_opened_at_month,
    DATE_FORMAT(opened_at_month, 'MMM yyyy') AS opened_at_month_label
FROM with_windows
