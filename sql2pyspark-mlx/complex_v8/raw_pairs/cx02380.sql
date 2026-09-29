WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        CASE WHEN actor IN ('agent', 'customer') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM ticket_events
    WHERE event_type = 'reopened'
      AND ticket_id IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT *, DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM filtered
    WHERE ticket_id <= 55
),
actor_stats AS (
    -- Avg days_since_event_at per actor
    SELECT actor, AVG(days_since_event_at) AS avg_days_since_event_at
    FROM enriched
    GROUP BY actor
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_event_at,
        ROUND(s.days_since_event_at - g.avg_days_since_event_at, 2) AS days_since_event_at_minus_avg
    FROM enriched s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.days_since_event_at > g.avg_days_since_event_at
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_type,
        actor,
        actor_len,
        actor_group,
        event_type_prefix,
        avg_days_since_event_at,
        days_since_event_at_minus_avg,
        ROUND(PERCENT_RANK() OVER w_event_at_year_event_type_by, 3) AS event_at_percent_rank,
        LAST_VALUE(actor_len) IGNORE NULLS OVER (PARTITION BY YEAR(event_at), event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_actor_len,
        LAST_VALUE(avg_days_since_event_at) IGNORE NULLS OVER (ORDER BY actor_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_avg_days_since_event_at
    FROM above_average
    WINDOW w_event_at_year_event_type_by AS (PARTITION BY YEAR(event_at), event_type ORDER BY event_at, id)
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_type,
        actor_group,
        event_type_prefix,
        avg_days_since_event_at,
        days_since_event_at_minus_avg,
        event_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, actor_len ORDER BY last_known_avg_days_since_event_at, id), 3) AS last_known_avg_days_since_event_at_cume_dist,
        RANK() OVER (PARTITION BY event_type, actor_len ORDER BY last_known_avg_days_since_event_at) AS last_known_avg_days_since_event_at_rank,
        RANK() OVER w_actor_len_by_avg_days_since AS avg_days_since_event_at_rank,
        LAST_VALUE(last_known_actor_len) OVER (PARTITION BY event_type, actor ORDER BY last_known_actor_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_known_actor_len
    FROM with_windows
    WINDOW w_actor_len_by_avg_days_since AS (PARTITION BY actor_len ORDER BY avg_days_since_event_at ASC), w_event_type_actor_len_by_last_2 AS (PARTITION BY event_type, actor_len ORDER BY last_known_avg_days_since_event_at)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        actor_group,
        event_at_percent_rank,
        avg_days_since_event_at_rank,
        SIZE(COLLECT_SET(actor_group) OVER w_last_known_avg_days_since_ev) AS distinct_actor_group_in_last_known_avg_days_sinc,
        COUNT(*) OVER (PARTITION BY last_known_avg_days_since_event_at_rank) AS rows_in_last_known_avg_days_since_event_at_rank,
        ROUND(avg_days_since_event_at / NULLIF(SUM(avg_days_since_event_at) OVER (PARTITION BY last_known_avg_days_since_event_at_rank), 0), 4) AS avg_days_since_event_at_share_of_last_known_avg_
    FROM with_windows_2
    WINDOW w_last_known_avg_days_since_ev AS (PARTITION BY last_known_avg_days_since_event_at_rank)
)
-- Final projection
SELECT
    id,
    actor_group,
    event_at_percent_rank,
    rows_in_last_known_avg_days_since_event_at_rank,
    avg_days_since_event_at_share_of_last_known_avg_,
    CASE WHEN actor_group IN ('group_a', 'other') THEN 'group_a' WHEN actor_group = 'group_b' THEN 'group_b' ELSE 'other' END AS actor_group_group
FROM sequenced
WHERE distinct_actor_group_in_last_known_avg_days_sinc BETWEEN 2 AND 6
ORDER BY actor_group_group DESC, id DESC
