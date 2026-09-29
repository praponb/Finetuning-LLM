WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        DATE_TRUNC('MONTH', event_at) AS event_at_month
    FROM ticket_events
    WHERE actor IS NOT NULL
      AND event_type IN ('created', 'replied', 'reopened')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY actor ORDER BY event_at) AS event_at_dense_rank,
        ROUND(CUME_DIST() OVER w_actor_by_event_at, 3) AS event_at_cume_dist,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY event_at), 3) AS event_at_cume_dist_2
    FROM cleaned
    WINDOW w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_slug,
        event_at_month,
        event_at_cume_dist,
        second_event_at,
        event_at_cume_dist_2
    FROM with_windows
    WHERE event_at_dense_rank = 1
      AND event_at_cume_dist >= 0.4
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        event_at_cume_dist_2 + event_at_cume_dist AS event_at_cume_dist_2_plus_event_at_cume_dist,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref,
        ROUND(event_at_cume_dist_2 * 1.07, 2) AS event_at_cume_dist_2_adj
    FROM top_per_group
    WHERE event_type IN ('reopened', 'assigned')
),
ticket_id_stats AS (
    -- Max event_at_cume_dist_2 per ticket_id
    SELECT ticket_id, MAX(event_at_cume_dist_2) AS max_event_at_cume_dist_2
    FROM scored
    GROUP BY ticket_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_event_at_cume_dist_2,
        ROUND(s.event_at_cume_dist_2 - g.max_event_at_cume_dist_2, 2) AS event_at_cume_dist_2_minus_max
    FROM scored s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.event_at_cume_dist_2 = g.max_event_at_cume_dist_2
),
unique_rows AS (
    -- Keep the earliest row per ticket_id, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_slug,
        event_at_month,
        event_at_cume_dist,
        second_event_at,
        event_at_cume_dist_2,
        event_at_cume_dist_2_plus_event_at_cume_dist,
        event_type_ref,
        event_at_cume_dist_2_adj,
        max_event_at_cume_dist_2,
        event_at_cume_dist_2_minus_max
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY second_event_at, id) AS row_num
        FROM compared
    ) ranked
    WHERE row_num = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type,
        event_type_slug,
        event_at_cume_dist,
        event_at_cume_dist_2_adj,
        max_event_at_cume_dist_2,
        event_at_cume_dist_2_minus_max,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at_cume_dist_2_adj ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY actor, event_type)) AS distinct_ticket_id_in_actor_event_type,
        MAX(event_at_cume_dist) OVER (ORDER BY event_at_cume_dist_2_minus_max ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS event_at_cume_dist_max_3,
        ROUND(PERCENT_RANK() OVER w_all_by_event_at_cume_dist_2_3, 3) AS event_at_cume_dist_2_adj_percent_rank,
        MAX(event_at_cume_dist_2_adj) OVER (ORDER BY event_at_cume_dist_2_minus_max ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_event_at_cume_dist_2_adj_so_far
    FROM unique_rows
    WINDOW w_all_by_event_at_cume_dist_2_3 AS (ORDER BY event_at_cume_dist_2_adj ASC)
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    event_type_slug,
    event_at_cume_dist,
    event_at_cume_dist_2_adj,
    max_event_at_cume_dist_2,
    event_at_cume_dist_2_minus_max,
    last_known_event_at,
    event_at_cume_dist_max_3,
    DAYOFWEEK(last_known_event_at) AS last_known_event_at_dayofweek
FROM windowed
ORDER BY last_known_event_at DESC NULLS FIRST, event_at_cume_dist_max_3 DESC NULLS FIRST
