WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE event_type <> 'assigned'
      AND TO_DATE(event_at) < DATE '2025-03-14'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        event_type,
        event_at_hour,
        event_type_ref,
        LAG(event_at, 2) OVER w_event_type_ticket_id_by_even AS prev2_event_at,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at_hour DESC, id) AS event_at_hour_rank,
        LAST_VALUE(event_at_hour) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_hour,
        ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id) AS seq_no,
        LAST_VALUE(event_at_hour) OVER (PARTITION BY ticket_id ORDER BY event_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_hour,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at_hour), 3) AS event_at_hour_percent_rank,
        LAST_VALUE(event_at_hour) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_hour_2,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at_hour DESC), 3) AS event_at_hour_cume_dist,
        ROUND((UNIX_TIMESTAMP(event_at_hour) - UNIX_TIMESTAMP(LAG(event_at_hour) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at_hour,
        NTILE(10) OVER (PARTITION BY ticket_id ORDER BY event_at_hour DESC, id) AS event_at_hour_decile,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at_hour DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LEAD(event_at, 3) OVER (PARTITION BY ticket_id ORDER BY event_at_hour, id) AS next3_event_at
    FROM prepared_events
    WINDOW w_event_type_ticket_id_by_even AS (PARTITION BY event_type, ticket_id ORDER BY event_at, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(last_event_at_hour_2) - UNIX_TIMESTAMP(last_known_event_at_hour)) / 3600, 1) AS hours_last_known_event_at_hour_to_last_event_at_,
        LENGTH(TRIM(event_type)) AS event_type_len,
        DATE_FORMAT(prev2_event_at, 'yyyy-MM') AS prev2_event_at_label
    FROM ranked
    WHERE event_at_hour >= TIMESTAMP '2025-01-25 09:00:00'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        event_type,
        event_type_ref,
        prev2_event_at,
        event_at_hour_rank,
        seq_no,
        event_at_hour_percent_rank,
        last_event_at_hour_2,
        event_at_hour_cume_dist,
        minutes_since_prev_event_at_hour,
        event_at_hour_decile,
        next3_event_at,
        hours_last_known_event_at_hour_to_last_event_at_,
        prev2_event_at_label,
        ROUND(event_at_hour_percent_rank - AVG(event_at_hour_percent_rank) OVER (PARTITION BY event_at_hour_decile, seq_no), 2) AS event_at_hour_percent_rank_vs_event_at_hour_deci,
        ROUND(event_type_len - AVG(event_type_len) OVER (PARTITION BY event_at_hour_decile, seq_no), 2) AS event_type_len_vs_event_at_hour_decile_seq_no_av,
        ROUND(event_type_len / NULLIF(SUM(event_type_len) OVER (PARTITION BY event_at_hour_decile, seq_no), 0), 4) AS event_type_len_share_of_event_at_hour_decile_seq,
        ROUND(event_at_hour_decile - AVG(event_at_hour_decile) OVER w_event_at_hour_decile_seq_no, 2) AS event_at_hour_decile_vs_event_at_hour_decile_seq,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', next3_event_at), event_type ORDER BY prev2_event_at ASC, id) AS rn,
        FIRST_VALUE(last_event_at_hour) OVER (ORDER BY event_at_hour_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_last_event_at_hour
    FROM derived
    WINDOW w_event_at_hour_decile_seq_no AS (PARTITION BY event_at_hour_decile, seq_no)
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    event_type_ref,
    prev2_event_at,
    event_at_hour_rank,
    seq_no,
    event_at_hour_percent_rank,
    last_event_at_hour_2,
    event_at_hour_decile,
    next3_event_at,
    prev2_event_at_label,
    event_at_hour_percent_rank_vs_event_at_hour_deci,
    event_type_len_vs_event_at_hour_decile_seq_no_av,
    event_type_len_share_of_event_at_hour_decile_seq,
    event_at_hour_decile_vs_event_at_hour_decile_seq,
    rn,
    first_last_event_at_hour
FROM sequenced
ORDER BY prev2_event_at DESC
