WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text,
        TO_DATE(opened_at) AS opened_at_date,
        DATE_TRUNC('YEAR', opened_at) AS opened_at_year
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'bug')
      AND HOUR(closed_at) BETWEEN 10 AND 14
),
agent_id_stats AS (
    -- Avg csat per agent_id
    SELECT agent_id, AVG(csat) AS avg_csat
    FROM filtered_tickets
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM filtered_tickets s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.csat > g.avg_csat
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags_text,
        s.opened_at_date,
        s.avg_csat,
        s.csat_minus_avg,
        su.agent_name,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM outliers s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
labeled AS (
    -- Add calculated columns
    SELECT *, LAST_DAY(agent_hired_on) AS agent_hired_on_month_end
    FROM combined_details
)
SELECT
    id,
    agent_id,
    closed_at,
    channel,
    csat,
    tags_text,
    opened_at_date,
    avg_csat,
    csat_minus_avg,
    agent_hired_on,
    agent_hired_on_month_end,
    closed_at + INTERVAL 30 MINUTES AS closed_at_plus_30m,
    LEAST(csat_minus_avg, avg_csat) AS least_csat_minus_avg_avg_csat
FROM labeled
