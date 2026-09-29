WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE event_type = 'assigned'
      AND actor = 'agent'
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, t.closed_at AS ticket_closed_at
    FROM filtered_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_type_len,
        ticket_closed_at,
        NTILE(10) OVER (PARTITION BY actor ORDER BY event_type_len, id) AS event_type_len_decile,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_type, event_type_len)) AS distinct_actor_in_event_type_event_type_len,
        MIN(event_type_len) OVER (PARTITION BY event_type, event_type_len) AS event_type_event_type_len_min_event_type_len,
        ROUND(event_type_len - AVG(event_type_len) OVER w_event_type_event_type_len, 2) AS event_type_len_vs_event_type_event_type_len_avg,
        event_type_len - LAG(event_type_len) OVER (PARTITION BY actor ORDER BY event_type_len, id) AS event_type_len_change,
        ROUND(CUME_DIST() OVER w_actor_event_at_year_by_ticke, 3) AS ticket_closed_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor ORDER BY event_type_len), 3) AS event_type_len_percent_rank,
        ROUND(AVG(event_type_len) OVER (PARTITION BY actor ORDER BY ticket_closed_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS event_type_len_avg_4,
        ROUND(CUME_DIST() OVER w_actor_by_ticket_closed_at, 3) AS ticket_closed_at_cume_dist_2,
        event_type_len - LAG(event_type_len) OVER (PARTITION BY actor ORDER BY ticket_closed_at DESC, id) AS event_type_len_change_2,
        COUNT(*) OVER (PARTITION BY event_type, event_type_len) AS rows_in_event_type_event_type_len
    FROM joined
    WINDOW w_event_type_event_type_len AS (PARTITION BY event_type, event_type_len), w_actor_event_at_year_by_ticke AS (PARTITION BY actor, YEAR(event_at) ORDER BY ticket_closed_at DESC), w_actor_by_ticket_closed_at AS (PARTITION BY actor ORDER BY ticket_closed_at DESC, id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(ticket_closed_at_cume_dist - AVG(ticket_closed_at_cume_dist) OVER (PARTITION BY event_type, YEAR(event_at)), 2) AS ticket_closed_at_cume_dist_vs_event_type_event_a,
        ROW_NUMBER() OVER (ORDER BY event_type_len_change_2 DESC, id) AS rn,
        event_type_len_change - LAG(event_type_len_change) OVER w_event_type_len_decile_by_row AS event_type_len_change_change,
        COUNT(*) OVER (PARTITION BY event_type, YEAR(event_at)) AS rows_in_event_type_event_at_year,
        event_type_len_percent_rank - LAG(event_type_len_percent_rank) OVER w_event_type_len_decile_by_row AS event_type_len_percent_rank_change,
        ROUND(AVG(ticket_closed_at_cume_dist) OVER (ORDER BY event_type_len_change_2 DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ticket_closed_at_cume_dist_avg_7,
        COUNT(*) OVER (PARTITION BY event_type_len_decile ORDER BY rows_in_event_type_event_type_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM sequenced
    WINDOW w_event_type_len_decile_by_row AS (PARTITION BY event_type_len_decile ORDER BY rows_in_event_type_event_type_len ASC, id)
)
-- Final projection
SELECT
    id,
    event_type,
    event_type_len,
    ticket_closed_at,
    event_type_event_type_len_min_event_type_len,
    event_type_len_percent_rank,
    rows_in_event_type_event_type_len,
    ticket_closed_at_cume_dist_vs_event_type_event_a,
    rn,
    event_type_len_change_change,
    event_type_len_percent_rank_change,
    running_count,
    event_type_len - rn AS event_type_len_minus_rn,
    CASE WHEN event_type_len_avg_4 >= 12.2 THEN 'A' WHEN event_type_len_avg_4 >= 6.2 THEN 'B' ELSE 'C' END AS event_type_len_avg_4_tier
FROM ranked
