WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix
    FROM ticket_events
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND HOUR(t.event_at) BETWEEN 7 AND 11
              AND t.actor = 'agent'
        )
      AND s.event_at < TIMESTAMP '2025-02-12 11:00:00'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_plus_30m,
        LAG(event_at_plus_30m) OVER w_all_by_event_at AS prev_event_at_plus_30m,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LEAD(event_at) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS next_event_at
    FROM screened
    WINDOW w_all_by_event_at AS (ORDER BY event_at ASC, id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY first_known_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_2,
        LAST_VALUE(event_at_plus_30m) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_plus_30m,
        ROUND(PERCENT_RANK() OVER (ORDER BY first_known_event_at DESC, id), 3) AS first_known_event_at_percent_rank,
        RANK() OVER w_ticket_id_by_event_at_ties AS event_at_rank
    FROM windowed
    WINDOW w_ticket_id_by_event_at_ties AS (PARTITION BY ticket_id ORDER BY event_at)
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor <> 'system'
        )
),
stats AS (
    -- Summarise per group
    SELECT
        event_type,
        ROUND(SUM(event_at_rank) / NULLIF(SUM(first_known_event_at_percent_rank), 0), 3) AS event_at_rank_to_first_known_event_at_percent_ra,
        COUNT(*) AS cnt,
        ROUND(SUM(first_known_event_at_percent_rank), 2) AS total_first_known_event_at_percent_rank,
        PERCENTILE_APPROX(event_at_rank, 0.5) AS median_event_at_rank
    FROM filtered
    GROUP BY 1
)
-- Final projection
SELECT
    event_type,
    event_at_rank_to_first_known_event_at_percent_ra,
    cnt,
    total_first_known_event_at_percent_rank,
    median_event_at_rank,
    median_event_at_rank + cnt AS median_event_at_rank_plus_cnt,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
FROM stats
WHERE total_first_known_event_at_percent_rank >= 1.8
ORDER BY event_at_rank_to_first_known_event_at_percent_ra DESC, median_event_at_rank_plus_cnt
