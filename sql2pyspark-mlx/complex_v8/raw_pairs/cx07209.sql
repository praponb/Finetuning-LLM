WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags
    FROM tickets
    WHERE csat <= 4
      AND agent_id IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority, TO_DATE(closed_at))) AS distinct_agent_id_in_priority_closed_at_to_date,
        RANK() OVER (PARTITION BY priority ORDER BY closed_at, id) AS closed_at_rank,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        ROUND(AVG(requester_id) OVER w_priority_closed_at_to_date, 2) AS priority_closed_at_to_date_avg_requester_id
    FROM cleaned
    WINDOW w_priority_closed_at_to_date AS (PARTITION BY priority, TO_DATE(closed_at))
),
agent_id_stats AS (
    -- Avg requester_id per agent_id
    SELECT agent_id, AVG(requester_id) AS avg_requester_id
    FROM windowed
    GROUP BY agent_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM windowed s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.requester_id > g.avg_requester_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at_2
    FROM above_average s
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM profiled s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
combined_details_agent_2 AS (
    -- Attach agent attributes
    SELECT s.*, su.agent_name AS agent_agent_name, su.tier AS agent_tier
    FROM combined_details_agent s
    JOIN support_agents su ON su.id = s.agent_id
),
totals AS (
    -- Summarise per group
    SELECT
        TO_DATE(last_known_opened_at) AS last_known_opened_at_day,
        distinct_agent_id_in_priority_closed_at_to_date,
        DATE_TRUNC('MONTH', first_ticket_closed_at_2) AS first_ticket_closed_at_2_month,
        ROUND(SUM(requester_id_minus_avg), 2) AS total_requester_id_minus_avg,
        ROUND(SUM(avg_requester_id) / NULLIF(SUM(distinct_agent_id_in_priority_closed_at_to_date), 0), 3) AS avg_requester_id_to_distinct_agent_id_in_priorit,
        ROUND(SUM(requester_id) / NULLIF(SUM(priority_closed_at_to_date_avg_requester_id), 0), 3) AS requester_id_to_priority_closed_at_to_date_avg_r
    FROM combined_details_agent_2
    WHERE requester_id_minus_avg BETWEEN -17.5 AND 10.0
    GROUP BY TO_DATE(last_known_opened_at), distinct_agent_id_in_priority_closed_at_to_date, DATE_TRUNC('MONTH', first_ticket_closed_at_2)
)
SELECT
    last_known_opened_at_day,
    distinct_agent_id_in_priority_closed_at_to_date,
    total_requester_id_minus_avg,
    requester_id_to_priority_closed_at_to_date_avg_r,
    DATE_FORMAT(last_known_opened_at_day, 'yyyy-MM-dd') AS last_known_opened_at_day_label,
    QUARTER(last_known_opened_at_day) AS last_known_opened_at_day_quarter
FROM totals
ORDER BY requester_id_to_priority_closed_at_to_date_avg_r, distinct_agent_id_in_priority_closed_at_to_date
