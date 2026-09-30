WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM ticket_events
    WHERE (event_type NOT IN ('created', 'resolved') OR actor NOT IN ('customer', 'system'))
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.priority AS ticket_priority,
        t.agent_id AS ticket_agent_id,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ticket_agent_id ORDER BY event_at DESC, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER w_all_by_agent_hired_on_ties, 3) AS agent_hired_on_percent_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_priority ORDER BY agent_hired_on ASC, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_agent_id ORDER BY agent_hired_on DESC, id), 3) AS agent_hired_on_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY ticket_agent_id ORDER BY agent_hired_on DESC, id) AS rn,
        LEAD(event_at, 2) OVER (PARTITION BY ticket_agent_id ORDER BY agent_hired_on DESC, id) AS next2_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_agent_id, actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LEAD(event_at, 2) OVER (ORDER BY agent_hired_on DESC, id) AS next2_event_at_2,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_agent_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAG(event_at) OVER (PARTITION BY ticket_agent_id ORDER BY agent_hired_on DESC, id) AS prev_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_agent_id, actor ORDER BY event_at, id) AS seq_no_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_ticket_agent_id_by_event_at)) / 60, 1) AS minutes_since_prev_event_at_2
    FROM joined
    WINDOW w_all_by_agent_hired_on_ties AS (ORDER BY agent_hired_on DESC), w_ticket_agent_id_by_event_at AS (PARTITION BY ticket_agent_id ORDER BY event_at DESC, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_team,
        DATE_TRUNC('MONTH', last_known_event_at) AS last_known_event_at_month,
        actor,
        MAX(seq_no) AS max_seq_no,
        ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list,
        COUNT(DISTINCT ticket_priority) AS distinct_ticket_prioritys,
        ROUND(AVG(agent_hired_on_percent_rank), 2) AS avg_agent_hired_on_percent_rank
    FROM windowed
    GROUP BY 1, 2, 3
    HAVING MAX(agent_hired_on_percent_rank) > 0.1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        agent_team,
        last_known_event_at_month,
        actor,
        max_seq_no,
        agent_team_list,
        distinct_ticket_prioritys,
        avg_agent_hired_on_percent_rank,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_event_at_month), distinct_ticket_prioritys ORDER BY max_seq_no ASC, agent_team, last_known_event_at_month, actor) AS max_seq_no_rank,
        FIRST_VALUE(distinct_ticket_prioritys) IGNORE NULLS OVER (PARTITION BY distinct_ticket_prioritys ORDER BY max_seq_no, agent_team, last_known_event_at_month, actor ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_distinct_ticket_prioritys,
        NTILE(3) OVER w_last_known_event_at_month_mo AS max_seq_no_tercile
    FROM stats
    WINDOW w_last_known_event_at_month_mo AS (PARTITION BY DATE_TRUNC('MONTH', last_known_event_at_month), distinct_ticket_prioritys ORDER BY max_seq_no ASC, agent_team, last_known_event_at_month, actor)
)
SELECT
    agent_team,
    last_known_event_at_month,
    max_seq_no,
    distinct_ticket_prioritys,
    avg_agent_hired_on_percent_rank,
    max_seq_no_rank,
    first_known_distinct_ticket_prioritys,
    max_seq_no_tercile,
    FLOOR(max_seq_no_rank / 2) * 2 AS max_seq_no_rank_bucket
FROM with_windows
WHERE actor = 'agent'
ORDER BY agent_team, max_seq_no_rank_bucket
