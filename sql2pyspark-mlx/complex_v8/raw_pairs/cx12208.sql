WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        csat,
        DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label
    FROM tickets
    WHERE agent_id >= 6
),
rollup_base AS (
    -- Group-level totals
    SELECT
        csat,
        priority,
        agent_id,
        SUM(requester_id) AS total_requester_id,
        SUM(CASE WHEN priority = 'P3' THEN 1 ELSE 0 END) AS n_priority_p3,
        COUNT(*) AS cnt,
        ROUND(SUM(requester_id) / NULLIF(SUM(csat), 0), 3) AS requester_id_to_csat_ratio
    FROM prepared_tickets
    GROUP BY 1, 2, 3
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM rollup_base s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
        )
      AND s.cnt <= 8
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count_2,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count_3
    FROM screened s
),
detail_rows AS (
    -- One row per ticket
    SELECT
        s.csat,
        s.priority,
        s.agent_id,
        s.total_requester_id,
        s.n_priority_p3,
        s.ticket_count,
        s.ticket_count_2,
        t.id AS ticket_id,
        t.closed_at AS ticket_closed_at,
        t.requester_id AS ticket_requester_id
    FROM profiled s
    JOIN tickets t ON t.agent_id = s.agent_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        csat,
        priority,
        agent_id,
        total_requester_id,
        n_priority_p3,
        ticket_count,
        ticket_count_2,
        ticket_id,
        ticket_requester_id,
        COUNT(*) OVER (PARTITION BY n_priority_p3) AS rows_in_n_priority_p3,
        NTILE(10) OVER w_ticket_count_agent_id_by_tic AS ticket_requester_id_decile,
        FIRST_VALUE(ticket_requester_id) IGNORE NULLS OVER (PARTITION BY priority ORDER BY total_requester_id DESC NULLS FIRST, csat, priority, agent_id, ticket_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_requester_id,
        LAG(ticket_count, 1, 0) OVER (PARTITION BY ticket_count, agent_id ORDER BY ticket_requester_id ASC, csat, priority, agent_id, ticket_id) AS prev_ticket_count,
        FIRST_VALUE(total_requester_id) IGNORE NULLS OVER (PARTITION BY priority ORDER BY total_requester_id DESC NULLS FIRST, csat, priority, agent_id, ticket_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_total_requester_id,
        FIRST_VALUE(ticket_requester_id) OVER (PARTITION BY priority ORDER BY total_requester_id DESC NULLS FIRST, csat, priority, agent_id, ticket_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_requester_id
    FROM detail_rows
    WINDOW w_ticket_count_agent_id_by_tic AS (PARTITION BY ticket_count, agent_id ORDER BY ticket_requester_id ASC, csat, priority, agent_id, ticket_id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(prev_ticket_count) OVER w_ticket_count AS ticket_count_max_prev_ticket_count,
        NTILE(3) OVER (PARTITION BY csat, prev_ticket_count ORDER BY ticket_requester_id NULLS LAST, csat, priority, agent_id, ticket_id) AS ticket_requester_id_tercile,
        DENSE_RANK() OVER (PARTITION BY csat, prev_ticket_count ORDER BY ticket_requester_id NULLS LAST, csat, priority, agent_id, ticket_id) AS ticket_requester_id_dense_rank
    FROM sequenced
    WINDOW w_ticket_count AS (PARTITION BY ticket_count)
)
SELECT
    csat,
    priority,
    total_requester_id,
    ticket_count,
    ticket_id,
    rows_in_n_priority_p3,
    ticket_requester_id_decile,
    first_known_ticket_requester_id,
    prev_ticket_count,
    first_known_total_requester_id,
    first_ticket_requester_id,
    ticket_count_max_prev_ticket_count,
    ticket_requester_id_tercile,
    ticket_requester_id_dense_rank,
    LENGTH(TRIM(priority)) AS priority_len,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
FROM metrics
WHERE ticket_count_2 < 6
