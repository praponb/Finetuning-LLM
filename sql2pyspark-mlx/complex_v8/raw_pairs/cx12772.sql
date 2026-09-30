WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('agent', 'system') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE event_type NOT IN ('created', 'assigned')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_actor_by_event_at_ties, 3) AS event_at_percent_rank,
        RANK() OVER (PARTITION BY actor_or_unknown ORDER BY event_at) AS event_at_rank,
        NTILE(3) OVER (ORDER BY event_at ASC, id) AS event_at_tercile,
        ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at, id) AS seq_no,
        RANK() OVER (PARTITION BY actor ORDER BY event_at, id) AS event_at_rank_2,
        NTILE(4) OVER (ORDER BY event_at ASC, id) AS event_at_quartile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        RANK() OVER w_event_type_by_event_at AS event_at_rank_3
    FROM base
    WINDOW w_actor_by_event_at_ties AS (PARTITION BY actor ORDER BY event_at), w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_group,
        actor_or_unknown,
        event_at_percent_rank,
        event_at_rank,
        event_at_tercile,
        seq_no,
        event_at_quartile,
        minutes_since_prev_event_at,
        event_at_rank_3
    FROM ranked
    WHERE event_at_rank_2 = 1
      AND actor_or_unknown = 'system'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(event_at_tercile / NULLIF(SUM(event_at_tercile) OVER (PARTITION BY event_at_rank), 0), 4) AS event_at_tercile_share_of_event_at_rank,
        RANK() OVER (PARTITION BY event_at_quartile, event_at_tercile ORDER BY minutes_since_prev_event_at NULLS LAST) AS minutes_since_prev_event_at_rank,
        ROUND(AVG(event_at_tercile) OVER (PARTITION BY event_at_quartile ORDER BY event_at_rank_3, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS event_at_tercile_avg_7,
        ROUND(CUME_DIST() OVER (PARTITION BY event_at_quartile, event_at_tercile ORDER BY minutes_since_prev_event_at NULLS LAST, id), 3) AS minutes_since_prev_event_at_cume_dist
    FROM top_rows
),
totals AS (
    -- Summarise per group
    SELECT
        actor_or_unknown,
        TO_DATE(event_at) AS event_at_day,
        ROUND(AVG(minutes_since_prev_event_at_rank), 2) AS avg_minutes_since_prev_event_at_rank,
        SUM(minutes_since_prev_event_at_cume_dist) AS total_minutes_since_prev_event_at_cume_dist,
        ROUND(100 * COUNT(CASE WHEN actor IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_actor_is_not_null
    FROM with_windows
    GROUP BY actor_or_unknown, TO_DATE(event_at)
    HAVING MAX(minutes_since_prev_event_at_rank) > 4
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(pct_actor_is_not_null) OVER (PARTITION BY actor_or_unknown) AS actor_or_unknown_sum_pct_actor_is_not_null,
        LEAD(event_at_day, 2) OVER w_all_by_pct_actor_is_not_null AS next2_event_at_day,
        LEAD(pct_actor_is_not_null, 2) OVER (ORDER BY pct_actor_is_not_null, actor_or_unknown, event_at_day) AS next2_pct_actor_is_not_null
    FROM totals
    WINDOW w_all_by_pct_actor_is_not_null AS (ORDER BY pct_actor_is_not_null, actor_or_unknown, event_at_day)
)
-- Final projection
SELECT
    actor_or_unknown,
    event_at_day,
    avg_minutes_since_prev_event_at_rank,
    pct_actor_is_not_null,
    next2_event_at_day,
    LOWER(REGEXP_REPLACE(TRIM(actor_or_unknown), ' ', '_')) AS actor_or_unknown_slug,
    DATE_FORMAT(next2_event_at_day, 'yyyy-MM-dd') AS next2_event_at_day_label
FROM with_trends
ORDER BY avg_minutes_since_prev_event_at_rank NULLS LAST, next2_event_at_day
