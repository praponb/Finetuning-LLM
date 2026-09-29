WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('reopened', 'created') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE (actor = 'customer' OR HOUR(event_at) BETWEEN 7 AND 12)
),
tagged AS (
    -- Derive helper columns
    SELECT *, DATE_FORMAT(event_at_plus_30m, 'yyyy-MM') AS event_at_plus_30m_label
    FROM recent
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_group,
        s.event_at_plus_30m,
        s.event_type_len,
        t.opened_at AS ticket_opened_at,
        su.hired_on AS agent_hired_on
    FROM tagged s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM joined s
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(ticket_opened_at) OVER (PARTITION BY event_type_len, ticket_id ORDER BY ticket_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_opened_at,
        ROUND(AVG(event_count) OVER (PARTITION BY event_type_len, ticket_id ORDER BY ticket_opened_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS event_count_avg_6_prior,
        ROUND(event_count / NULLIF(SUM(event_count) OVER (PARTITION BY event_type_len), 0), 4) AS event_count_share_of_event_type_len,
        MAX(event_count) OVER (PARTITION BY event_type_len, ticket_id ORDER BY ticket_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_event_count_so_far,
        LAG(event_count, 3) OVER (PARTITION BY event_type_len, ticket_id ORDER BY ticket_opened_at DESC, id) AS prev3_event_count
    FROM profiled
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    event_type_group,
    event_at_plus_30m,
    event_type_len,
    event_count,
    event_count_avg_6_prior,
    event_count_share_of_event_type_len,
    max_event_count_so_far,
    prev3_event_count,
    DATE_TRUNC('HOUR', event_at_plus_30m) AS event_at_plus_30m_hour
FROM ranked
ORDER BY max_event_count_so_far, event_count
