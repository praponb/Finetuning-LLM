WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        csat,
        CASE WHEN channel IN ('chat', 'web') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group,
        DAYOFWEEK(opened_at) AS opened_at_dayofweek,
        DATE_TRUNC('QUARTER', closed_at) AS closed_at_quarter
    FROM tickets
    WHERE priority NOT IN ('P1', 'P2')
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.csat,
        s.channel_group,
        s.closed_at_quarter,
        t.id AS event_id,
        t.event_type,
        t.event_at,
        t.actor AS event_actor
    FROM filtered s
    JOIN ticket_events t ON t.ticket_id = s.id
),
totals AS (
    -- Summarise per group
    SELECT
        event_actor,
        YEAR(event_at) AS event_at_year,
        PERCENTILE_APPROX(csat, 0.5) AS median_csat,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT channel_group) AS distinct_channel_groups
    FROM line_level_events
    GROUP BY event_actor, YEAR(event_at)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE median_csat > (SELECT AVG(median_csat) FROM totals)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_distinct_channel_groups_medi, 3) AS median_csat_percent_rank,
        LEAD(distinct_channel_groups) OVER (PARTITION BY event_actor, median_csat ORDER BY distinct_channel_groups DESC, event_actor, event_at_year) AS next_distinct_channel_groups,
        distinct_channel_groups - LAG(distinct_channel_groups) OVER (PARTITION BY distinct_channel_groups, median_csat ORDER BY median_csat NULLS LAST, event_actor, event_at_year) AS distinct_channel_groups_change,
        event_at_year - LAG(event_at_year) OVER (PARTITION BY distinct_channel_groups, median_csat ORDER BY median_csat NULLS LAST, event_actor, event_at_year) AS event_at_year_change,
        MAX(median_csat) OVER w_event_actor AS event_actor_max_median_csat,
        NTH_VALUE(distinct_channel_groups, 2) OVER (PARTITION BY distinct_channel_groups, median_csat ORDER BY median_csat NULLS LAST, event_actor, event_at_year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_channel_groups
    FROM qualified
    WINDOW w_distinct_channel_groups_medi AS (PARTITION BY distinct_channel_groups, median_csat ORDER BY median_csat NULLS LAST, event_actor, event_at_year), w_event_actor AS (PARTITION BY event_actor)
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM metrics
    WHERE event_at_year > (SELECT MIN(event_at_year) FROM metrics)
)
SELECT
    event_actor,
    event_at_year,
    median_csat,
    n_rows,
    distinct_channel_groups,
    median_csat_percent_rank,
    distinct_channel_groups_change,
    event_actor_max_median_csat,
    second_distinct_channel_groups,
    LEAST(median_csat_percent_rank, n_rows) AS least_median_csat_percent_rank_n_rows
FROM with_activity
