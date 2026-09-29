WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE event_type <> 'reopened'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        actor,
        event_at_week,
        FIRST_VALUE(event_at_week) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_week,
        LEAD(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at DESC NULLS FIRST, id) AS next_event_at,
        DENSE_RANK() OVER (PARTITION BY actor, event_type ORDER BY event_at DESC NULLS FIRST) AS event_at_dense_rank
    FROM valid
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(first_known_event_at_week) OVER (PARTITION BY MONTH(first_known_event_at_week), ticket_id ORDER BY event_at_dense_rank DESC, id) AS prev_first_known_event_at_week,
        ROW_NUMBER() OVER (PARTITION BY event_at_dense_rank ORDER BY event_at_dense_rank DESC, id) AS seq_no
    FROM metrics
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        actor,
        event_at_week,
        first_known_event_at_week,
        next_event_at,
        event_at_dense_rank,
        prev_first_known_event_at_week,
        seq_no,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        LOWER(TRIM(actor)) AS actor_clean
    FROM sequenced
)
SELECT *
FROM (
    SELECT actor, next_event_at, actor_or_unknown, event_at_dense_rank
    FROM derived
) src
PIVOT (
    MIN(event_at_dense_rank)
    FOR actor_or_unknown IN ('UNKNOWN', 'customer')
)
ORDER BY actor, next_event_at
