WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-18 11:00:00'
),
with_parent AS (
    -- Join tickets
    SELECT s.*, t.requester_id AS ticket_requester_id
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.requester_id >= 6
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_type)) AS distinct_actor_in_event_type,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY actor, event_type ORDER BY actor_len DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ticket_requester_id_avg_2_prior,
        LEAD(event_at, 3) OVER (PARTITION BY event_type, actor ORDER BY ticket_requester_id DESC, id) AS next3_event_at,
        ROUND(actor_len / NULLIF(SUM(actor_len) OVER (PARTITION BY ticket_id), 0), 4) AS actor_len_share_of_ticket_id,
        FIRST_VALUE(actor_len) OVER (PARTITION BY event_type, actor ORDER BY ticket_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_actor_len,
        ticket_requester_id - LAG(ticket_requester_id) OVER (PARTITION BY actor, event_type ORDER BY actor_len DESC, id) AS ticket_requester_id_change,
        ROUND(actor_len / NULLIF(SUM(actor_len) OVER (PARTITION BY event_type), 0), 4) AS actor_len_share_of_event_type,
        MAX(ticket_requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), ticket_id ORDER BY actor_len, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ticket_requester_id_max_2,
        MAX(ticket_requester_id) OVER w_event_type AS event_type_max_ticket_requester_id,
        ROUND(CUME_DIST() OVER w_actor_len_event_type_by_acto, 3) AS actor_len_cume_dist
    FROM with_parent
    WINDOW w_event_type AS (PARTITION BY event_type), w_actor_len_event_type_by_acto AS (PARTITION BY actor_len, event_type ORDER BY actor_len)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(first_actor_len) OVER (PARTITION BY distinct_actor_in_event_type, first_actor_len) AS distinct_actor_in_event_type_first_actor_len_max,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS seq_no,
        NTH_VALUE(actor_len_cume_dist, 2) OVER (PARTITION BY actor, distinct_actor_in_event_type ORDER BY actor_len_share_of_event_type DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_actor_len_cume_dist,
        NTILE(3) OVER (PARTITION BY actor_len, YEAR(event_at) ORDER BY actor_len DESC, id) AS actor_len_tercile,
        ROUND(AVG(distinct_actor_in_event_type) OVER (ORDER BY event_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS distinct_actor_in_event_type_avg_2_prior,
        first_actor_len - LAG(first_actor_len) OVER (ORDER BY ticket_requester_id DESC NULLS FIRST, id) AS first_actor_len_change,
        ROUND(first_actor_len - AVG(first_actor_len) OVER (PARTITION BY distinct_actor_in_event_type), 2) AS first_actor_len_vs_distinct_actor_in_event_type_
    FROM with_windows
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_len,
    distinct_actor_in_event_type,
    next3_event_at,
    actor_len_share_of_ticket_id,
    first_actor_len,
    ticket_requester_id_change,
    ticket_requester_id_max_2,
    event_type_max_ticket_requester_id,
    actor_len_cume_dist,
    distinct_actor_in_event_type_first_actor_len_max,
    seq_no,
    second_actor_len_cume_dist,
    actor_len_tercile,
    distinct_actor_in_event_type_avg_2_prior,
    first_actor_len_change,
    DATE_TRUNC('DAY', event_at) AS event_at_day
FROM metrics
