WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE event_type <> 'reopened'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(event_at, 2) OVER w_actor_event_at_year_by_days AS prev2_event_at,
        LAST_VALUE(days_since_event_at) OVER (PARTITION BY actor, YEAR(event_at) ORDER BY days_since_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_event_at,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY actor_or_unknown, event_type)) AS distinct_ticket_id_in_actor_or_unknown_event_typ,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER (PARTITION BY actor_or_unknown, event_type), 0), 4) AS days_since_event_at_share_of_actor_or_unknown_ev,
        MIN(days_since_event_at) OVER (PARTITION BY actor_or_unknown, event_type) AS actor_or_unknown_event_type_min_days_since_event,
        RANK() OVER (ORDER BY days_since_event_at DESC, id) AS days_since_event_at_rank
    FROM prepared
    WINDOW w_actor_event_at_year_by_days AS (PARTITION BY actor, YEAR(event_at) ORDER BY days_since_event_at, id)
),
unique_rows AS (
    -- Keep the latest row per actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_or_unknown,
        actor_event_type_label,
        days_since_event_at,
        prev2_event_at,
        last_days_since_event_at,
        distinct_ticket_id_in_actor_or_unknown_event_typ,
        days_since_event_at_share_of_actor_or_unknown_ev,
        actor_or_unknown_event_type_min_days_since_event,
        days_since_event_at_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY distinct_ticket_id_in_actor_or_unknown_event_typ DESC, id DESC) AS row_num
        FROM metrics
    ) ranked
    WHERE row_num = 1
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_type,
        actor_event_type_label,
        prev2_event_at,
        last_days_since_event_at,
        distinct_ticket_id_in_actor_or_unknown_event_typ,
        days_since_event_at_share_of_actor_or_unknown_ev,
        actor_or_unknown_event_type_min_days_since_event,
        ROUND((UNIX_TIMESTAMP(prev2_event_at) - UNIX_TIMESTAMP(event_at)) / 3600, 1) AS hours_event_at_to_prev2_event_at,
        DATEDIFF(DATE '2025-12-31', prev2_event_at) AS days_since_prev2_event_at,
        ABS(days_since_event_at - 245) AS days_since_event_at_distance
    FROM unique_rows
)
SELECT id, measure, value
FROM (
    SELECT id, hours_event_at_to_prev2_event_at, days_since_event_at_share_of_actor_or_unknown_ev
    FROM scored
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (hours_event_at_to_prev2_event_at AS m_hours_event_at_to_prev2_event_at, days_since_event_at_share_of_actor_or_unknown_ev AS m_days_since_event_at_share_of_actor_or_unknown_)
) unp
ORDER BY id, measure
