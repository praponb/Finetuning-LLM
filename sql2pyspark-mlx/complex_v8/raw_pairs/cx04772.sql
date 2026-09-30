WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
        INITCAP(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor <> 'customer'
              AND t.event_at >= TIMESTAMP '2025-01-31 10:00:00'
        )
),
actor_stats AS (
    -- Avg actor_len per actor
    SELECT actor, AVG(actor_len) AS avg_actor_len
    FROM without_matches
    GROUP BY actor
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_actor_len,
        ROUND(s.actor_len - g.avg_actor_len, 2) AS actor_len_minus_avg
    FROM without_matches s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.actor_len > g.avg_actor_len
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        event_at,
        actor,
        actor_len,
        actor_len_minus_avg,
        ROUND(actor_len_minus_avg / NULLIF(actor_len, 0), 3) AS actor_len_minus_avg_per_actor_len
    FROM compared
)
SELECT
    id,
    event_at,
    actor,
    actor_len,
    actor_len_minus_avg_per_actor_len,
    ROUND(actor_len / NULLIF(actor_len_minus_avg, 0), 3) AS actor_len_per_actor_len_minus_avg,
    DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
FROM enriched
ORDER BY id, event_at_label DESC
LIMIT 20
