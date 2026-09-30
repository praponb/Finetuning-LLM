WITH matched AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        CASE WHEN hours_since_open >= 39.6 THEN 'large' WHEN hours_since_open >= 14.4 THEN 'medium' ELSE 'small' END AS hours_since_open_tier,
        ROUND(STDDEV(hours_since_open), 2) AS stddev_hours_since_open,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        MIN(hours_since_open) AS min_hours_since_open
    FROM matched
    GROUP BY CASE WHEN hours_since_open >= 39.6 THEN 'large' WHEN hours_since_open >= 14.4 THEN 'medium' ELSE 'small' END
    HAVING SUM(hours_since_open) > 10.8
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        hours_since_open_tier,
        priority_list,
        min_hours_since_open,
        stddev_hours_since_open - LAG(stddev_hours_since_open) OVER w_hours_since_open_tier_by_min AS stddev_hours_since_open_change,
        min_hours_since_open - LAG(min_hours_since_open) OVER w_hours_since_open_tier_by_min_2 AS min_hours_since_open_change
    FROM rollup_base
    WINDOW w_hours_since_open_tier_by_min AS (PARTITION BY hours_since_open_tier ORDER BY min_hours_since_open, hours_since_open_tier), w_hours_since_open_tier_by_min_2 AS (PARTITION BY hours_since_open_tier ORDER BY min_hours_since_open DESC, hours_since_open_tier)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY hours_since_open_tier ORDER BY min_hours_since_open DESC, hours_since_open_tier) AS min_hours_since_open_quartile,
        MAX(min_hours_since_open_change) OVER (PARTITION BY hours_since_open_tier) AS hours_since_open_tier_max_min_hours_since_open_c,
        min_hours_since_open - LAG(min_hours_since_open) OVER (PARTITION BY hours_since_open_tier ORDER BY min_hours_since_open ASC, hours_since_open_tier) AS min_hours_since_open_change_2,
        ROUND(AVG(min_hours_since_open_change) OVER (PARTITION BY hours_since_open_tier ORDER BY min_hours_since_open DESC, hours_since_open_tier ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS min_hours_since_open_change_avg_1_prior,
        FIRST_VALUE(min_hours_since_open_change) OVER (PARTITION BY hours_since_open_tier ORDER BY min_hours_since_open DESC, hours_since_open_tier ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_min_hours_since_open_change
    FROM with_trends
)
SELECT *
FROM (
    SELECT min_hours_since_open_quartile, hours_since_open_tier, min_hours_since_open_change
    FROM metrics
) src
PIVOT (
    SUM(min_hours_since_open_change)
    FOR hours_since_open_tier IN ('small', 'medium', 'large')
)
