WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE event_type <> 'replied'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_type_len,
        ROUND(AVG(event_type_len) OVER (ORDER BY event_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS event_type_len_avg_3,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY ticket_id)) AS distinct_actor_in_ticket_id,
        NTH_VALUE(event_type_len, 2) OVER (PARTITION BY ticket_id, actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_type_len,
        MIN(event_type_len) OVER (PARTITION BY MONTH(event_at)) AS event_at_month_min_event_type_len,
        SIZE(COLLECT_SET(event_type) OVER w_ticket_id) AS distinct_event_type_in_ticket_id,
        SUM(event_type_len) OVER w_event_type AS event_type_sum_event_type_len,
        ROUND(event_type_len - AVG(event_type_len) OVER (PARTITION BY ticket_id), 2) AS event_type_len_vs_ticket_id_avg,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTILE(10) OVER (ORDER BY event_at, id) AS event_at_decile,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY MONTH(event_at))) AS distinct_event_type_in_event_at_month
    FROM prepared_events
    WINDOW w_ticket_id AS (PARTITION BY ticket_id), w_event_type AS (PARTITION BY event_type)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_type_len,
        event_type_len_avg_3,
        distinct_actor_in_ticket_id,
        event_at_month_min_event_type_len,
        distinct_event_type_in_ticket_id,
        event_type_sum_event_type_len,
        event_type_len_vs_ticket_id_avg,
        first_event_at,
        event_at_decile,
        distinct_event_type_in_event_at_month,
        ROUND(AVG(event_type_len) OVER (ORDER BY event_type_len NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS event_type_len_avg_7,
        ROUND(AVG(event_at_decile) OVER (PARTITION BY distinct_event_type_in_event_at_month), 2) AS distinct_event_type_in_event_at_month_avg_event_,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', first_event_at) ORDER BY event_at_decile DESC, id) AS event_at_decile_seq,
        ROUND(CUME_DIST() OVER (ORDER BY event_type_len NULLS LAST, id), 3) AS event_type_len_cume_dist,
        COUNT(*) OVER (PARTITION BY ticket_id) AS rows_in_ticket_id
    FROM sequenced
)
-- Final projection
SELECT
    id,
    actor,
    event_type_len,
    event_type_len_avg_3,
    distinct_actor_in_ticket_id,
    event_at_month_min_event_type_len,
    distinct_event_type_in_ticket_id,
    event_type_sum_event_type_len,
    event_type_len_vs_ticket_id_avg,
    first_event_at,
    event_at_decile,
    distinct_event_type_in_event_at_month,
    event_type_len_avg_7,
    distinct_event_type_in_event_at_month_avg_event_,
    event_at_decile_seq,
    rows_in_ticket_id,
    SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix
FROM with_windows
ORDER BY event_type_sum_event_type_len DESC NULLS FIRST
