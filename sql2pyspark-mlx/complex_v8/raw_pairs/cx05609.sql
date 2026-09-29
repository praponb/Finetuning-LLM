WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-24'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        NTILE(3) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id) AS event_at_tercile,
        RANK() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC) AS event_at_rank,
        NTILE(4) OVER (ORDER BY event_at DESC, id) AS event_at_quartile,
        NTILE(10) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id) AS event_at_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id), 3) AS event_at_percent_rank_2,
        LAG(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS prev_event_at,
        LAST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM cleaned
),
leaders AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE event_at_rank = 1
),
with_windows_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY event_at_decile DESC, id) AS event_at_decile_seq,
        MIN(event_at_percent_rank_2) OVER w_event_at_decile AS event_at_decile_min_event_at_percent_rank_2,
        ROUND(CUME_DIST() OVER w_all_by_event_at_ties, 3) AS event_at_cume_dist,
        ROUND(AVG(event_at_decile) OVER (PARTITION BY event_at_decile), 2) AS event_at_decile_avg_event_at_decile,
        ROUND(event_at_percent_rank_2 - AVG(event_at_percent_rank_2) OVER (PARTITION BY YEAR(last_event_at)), 2) AS event_at_percent_rank_2_vs_last_event_at_year_av,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_at_decile)) AS distinct_actor_in_event_at_decile,
        event_at_tercile - LAG(event_at_tercile) OVER (PARTITION BY event_at_decile, event_at_tercile ORDER BY event_at_quartile NULLS LAST, id) AS event_at_tercile_change
    FROM leaders
    WINDOW w_event_at_decile AS (PARTITION BY event_at_decile), w_all_by_event_at_ties AS (ORDER BY event_at DESC)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    event_at_tercile,
    event_at_quartile,
    event_at_decile,
    event_at_percent_rank_2,
    prev_event_at,
    last_event_at,
    event_at_decile_avg_event_at_decile,
    distinct_actor_in_event_at_decile,
    event_at_tercile_change,
    ROUND(event_at_percent_rank_2 * 1.1, 2) AS event_at_percent_rank_2_adj,
    CASE WHEN event_at_decile >= 8 THEN 'large' WHEN event_at_decile >= 3 THEN 'medium' ELSE 'small' END AS event_at_decile_tier
FROM with_windows_2
ORDER BY ticket_id DESC NULLS FIRST, event_at_tercile
