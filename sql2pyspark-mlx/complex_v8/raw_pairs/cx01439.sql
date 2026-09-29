WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM scoped_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND HOUR(t.event_at) BETWEEN 8 AND 13
),
filtered AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM detail_rows_events s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.closed_at >= TIMESTAMP '2025-02-08 15:00:00'
              AND t.csat < 4
        )
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_at DESC, id, event_id) AS event_at_dense_rank,
        LAST_VALUE(event_event_at) IGNORE NULLS OVER (PARTITION BY actor, event_id ORDER BY event_at DESC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_event_at,
        RANK() OVER w_event_type_by_event_at_ties AS event_at_rank
    FROM filtered
    WINDOW w_event_type_by_event_at_ties AS (PARTITION BY event_type ORDER BY event_at DESC)
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at
    FROM with_trends s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_id,
        event_event_type,
        event_actor,
        event_at_dense_rank,
        last_known_event_event_at,
        first_event_at,
        ROUND(event_at_dense_rank / NULLIF(SUM(event_at_dense_rank) OVER (PARTITION BY event_at_dense_rank, event_at_rank), 0), 4) AS event_at_dense_rank_share_of_event_at_dense_rank,
        ROUND(AVG(event_at_rank) OVER (PARTITION BY event_at_dense_rank, event_at_rank), 2) AS event_at_dense_rank_event_at_rank_avg_event_at_r,
        ROUND(CUME_DIST() OVER w_event_actor_actor_by_last_kn, 3) AS last_known_event_event_at_cume_dist,
        NTILE(3) OVER (PARTITION BY event_actor, actor ORDER BY last_known_event_event_at DESC, id, event_id) AS last_known_event_event_at_tercile
    FROM with_stats
    WINDOW w_event_actor_actor_by_last_kn AS (PARTITION BY event_actor, actor ORDER BY last_known_event_event_at DESC, id, event_id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, event_id, metric, amount
FROM (
    SELECT id, event_id, event_at_dense_rank, last_known_event_event_at_tercile
    FROM sequenced
) src
UNPIVOT (
    amount FOR metric IN (event_at_dense_rank AS m_event_at_dense_rank, last_known_event_event_at_tercile AS m_last_known_event_event_at_tercile)
) unp
ORDER BY id, event_id, metric
