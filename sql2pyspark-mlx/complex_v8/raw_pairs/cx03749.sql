WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('resolved', 'replied') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        DATE_TRUNC('MONTH', event_at) AS event_at_month
    FROM ticket_events
    WHERE actor NOT IN ('agent', 'system')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, actor ORDER BY event_at ASC, id) AS seq_no,
        LEAD(event_at_month) OVER w_all_by_event_at AS next_event_at_month
    FROM recent
    WINDOW w_ticket_id_actor_by_event_at AS (PARTITION BY ticket_id, actor ORDER BY event_at ASC, id), w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_at_label,
        event_at_month,
        seq_no,
        next_event_at_month,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(AVG(seq_no) OVER (PARTITION BY ticket_id ORDER BY seq_no, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS seq_no_avg_3
    FROM sequenced
),
with_parent AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_at_label,
        s.next_event_at_month,
        s.seq_no_avg_3,
        t.opened_at AS ticket_opened_at,
        t.csat AS ticket_csat,
        t.tags AS ticket_tags
    FROM ranked s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
unique_rows AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_at_label,
        next_event_at_month,
        seq_no_avg_3,
        ticket_opened_at,
        ticket_csat,
        ticket_tags
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY ticket_opened_at DESC, id DESC) AS row_num
        FROM with_parent
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    event_at_label,
    next_event_at_month,
    seq_no_avg_3,
    ticket_opened_at,
    ticket_tags,
    DATE_FORMAT(next_event_at_month, 'yyyy-MM-dd') AS next_event_at_month_label
FROM unique_rows
