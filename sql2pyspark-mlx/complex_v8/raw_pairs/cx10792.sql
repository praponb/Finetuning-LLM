WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat - requester_id AS csat_minus_requester_id,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
    WHERE requester_id <= 21
      AND channel = 'web'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(csat_minus_requester_id) OVER (PARTITION BY priority, agent_id) AS priority_agent_id_max_csat_minus_requester_id,
        NTILE(5) OVER w_priority_by_csat_per_request AS csat_per_requester_id_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY priority ORDER BY csat_per_requester_id, id), 3) AS csat_per_requester_id_cume_dist,
        NTH_VALUE(csat_per_requester_id, 2) OVER (PARTITION BY priority ORDER BY csat_per_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat_per_requester_id,
        MAX(csat_per_requester_id) OVER (PARTITION BY priority ORDER BY csat_per_requester_id, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS csat_per_requester_id_max_3,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority ORDER BY csat_per_requester_id), 3) AS csat_per_requester_id_percent_rank
    FROM cleaned
    WINDOW w_priority_by_csat_per_request AS (PARTITION BY priority ORDER BY csat_per_requester_id, id)
),
joined_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat_minus_requester_id,
        s.csat_per_requester_id,
        s.priority_agent_id_max_csat_minus_requester_id,
        s.csat_per_requester_id_quintile,
        s.csat_per_requester_id_cume_dist,
        s.csat_per_requester_id_max_3,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM sequenced s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        MIN(closed_at) AS first_ticket_closed_at,
        COUNT(*) AS ticket_count_2
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.first_ticket_closed_at,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2
    FROM joined_agent s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(closed_at) AS closed_at_day,
        ticket_count_2,
        ROUND(SUM(priority_agent_id_max_csat_minus_requester_id) / NULLIF(SUM(csat_per_requester_id), 0), 3) AS priority_agent_id_max_csat_minus_requester_id_to,
        COUNT(*) AS n_rows
    FROM profiled
    GROUP BY TO_DATE(closed_at), ticket_count_2
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE priority_agent_id_max_csat_minus_requester_id_to > (SELECT AVG(priority_agent_id_max_csat_minus_requester_id_to) FROM stats)
),
labeled AS (
    -- Add calculated columns
    SELECT *, MONTH(closed_at_day) AS closed_at_day_month
    FROM filtered
    WHERE YEAR(closed_at_day) = 2025
)
-- Native SQL UNPIVOT (wide to long)
SELECT closed_at_day, ticket_count_2, measure, metric_value
FROM (
    SELECT closed_at_day, ticket_count_2, n_rows, closed_at_day_month
    FROM labeled
) src
UNPIVOT (
    metric_value FOR measure IN (n_rows AS n_rows_v, closed_at_day_month AS closed_at_day_month_v)
) unp
ORDER BY closed_at_day, ticket_count_2, measure
