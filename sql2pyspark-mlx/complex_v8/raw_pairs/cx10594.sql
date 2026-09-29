WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix,
        LOWER(TRIM(actor)) AS actor_clean,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM ticket_events
    WHERE actor IN ('customer', 'agent')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(event_at) OVER (ORDER BY event_at, id) AS prev_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, ticket_id ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROW_NUMBER() OVER w_actor_ticket_id_by_event_at_3 AS seq_no,
        NTILE(5) OVER (ORDER BY event_at, id) AS event_at_quintile
    FROM valid
    WINDOW w_actor_ticket_id_by_event_at_3 AS (PARTITION BY actor, ticket_id ORDER BY event_at NULLS LAST, id)
),
tagged AS (
    -- Derive helper columns
    SELECT *, DATE_FORMAT(prev_event_at, 'MMM yyyy') AS prev_event_at_label
    FROM windowed
    WHERE event_type_prefix IS NOT NULL
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    event_type_prefix,
    actor_clean,
    event_at_label,
    prev_event_at,
    second_event_at,
    second_event_at_2,
    first_known_event_at,
    seq_no,
    ABS(event_at_quintile - 5) AS event_at_quintile_distance,
    event_at_quintile - seq_no AS event_at_quintile_minus_seq_no
FROM tagged
ORDER BY actor_clean DESC, seq_no
