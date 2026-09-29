WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE actor = 'system'
),
filtered AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM base_events s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND MONTH(t.closed_at) IN (2, 3)
        )
),
filtered_2 AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.channel <> 'web'
              AND t.requester_id < 18
        )
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM filtered_2 s
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(last_event_at, 2) OVER (PARTITION BY event_type, TO_DATE(last_event_at) ORDER BY last_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_event_at,
        LEAD(last_event_at) OVER (PARTITION BY event_type, TO_DATE(last_event_at) ORDER BY last_event_at DESC, id) AS next_last_event_at,
        LAG(last_event_at) OVER (PARTITION BY event_type, TO_DATE(last_event_at) ORDER BY last_event_at DESC, id) AS prev_last_event_at,
        ROUND(AVG(event_count) OVER (PARTITION BY YEAR(event_at)), 2) AS event_at_year_avg_event_count,
        ROW_NUMBER() OVER (PARTITION BY event_type, TO_DATE(last_event_at) ORDER BY last_event_at DESC, id) AS last_event_at_seq
    FROM summarized_rows
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(prev_last_event_at, 2) OVER (PARTITION BY YEAR(last_event_at) ORDER BY event_at ASC, id) AS prev2_prev_last_event_at,
        ROUND(AVG(event_count) OVER (ORDER BY second_last_event_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS event_count_avg_1_prior,
        ROUND(CUME_DIST() OVER (ORDER BY second_last_event_at DESC), 3) AS second_last_event_at_cume_dist
    FROM with_trends
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    first_event_at,
    event_count,
    second_last_event_at,
    prev_last_event_at,
    last_event_at_seq,
    prev2_prev_last_event_at,
    event_count_avg_1_prior,
    LENGTH(TRIM(actor)) AS actor_len,
    SUBSTRING(TRIM(event_type_ref), 1, 3) AS event_type_ref_prefix
FROM sequenced
ORDER BY id DESC NULLS FIRST
