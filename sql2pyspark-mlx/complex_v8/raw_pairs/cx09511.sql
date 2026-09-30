WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'bug')
      AND csat > 2
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        FIRST_VALUE(closed_at) OVER (PARTITION BY agent_id, csat ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        FIRST_VALUE(csat) OVER (ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        ROUND(PERCENT_RANK() OVER w_all_by_csat_ties, 3) AS csat_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY csat NULLS LAST), 3) AS csat_cume_dist,
        FIRST_VALUE(csat) OVER (PARTITION BY agent_id, csat ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat_2,
        RANK() OVER w_all_by_csat AS csat_rank
    FROM filtered
    WINDOW w_all_by_csat_ties AS (ORDER BY csat NULLS LAST), w_all_by_csat AS (ORDER BY csat NULLS LAST, id)
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.csat,
        s.first_closed_at,
        s.csat_percent_rank,
        s.csat_cume_dist,
        s.first_csat_2,
        s.csat_rank,
        su.hired_on AS agent_hired_on
    FROM sequenced s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM combined_details_agent
    WHERE csat_rank <= 5
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(csat_cume_dist) OVER (PARTITION BY TO_DATE(first_closed_at), first_csat_2 ORDER BY first_closed_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS csat_cume_dist_avg_7,
        NTILE(5) OVER (PARTITION BY TO_DATE(first_closed_at), first_csat_2 ORDER BY first_closed_at DESC, id) AS first_closed_at_quintile,
        NTH_VALUE(csat_cume_dist, 2) OVER (ORDER BY csat_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat_cume_dist,
        ROUND((UNIX_TIMESTAMP(first_closed_at) - UNIX_TIMESTAMP(LAG(first_closed_at) OVER (PARTITION BY TO_DATE(first_closed_at), first_csat_2 ORDER BY first_closed_at DESC, id))) / 60, 1) AS minutes_since_prev_first_closed_at,
        NTH_VALUE(first_closed_at, 2) OVER (PARTITION BY TO_DATE(first_closed_at), first_csat_2 ORDER BY first_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_closed_at
    FROM leaders
),
agent_id_stats AS (
    -- Avg second_csat_cume_dist per agent_id
    SELECT agent_id, AVG(second_csat_cume_dist) AS avg_second_csat_cume_dist
    FROM with_windows
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_second_csat_cume_dist,
        ROUND(s.second_csat_cume_dist - g.avg_second_csat_cume_dist, 2) AS second_csat_cume_dist_minus_avg
    FROM with_windows s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.second_csat_cume_dist > g.avg_second_csat_cume_dist
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.csat,
        s.csat_percent_rank,
        s.second_csat_cume_dist,
        s.minutes_since_prev_first_closed_at,
        s.second_first_closed_at,
        s.avg_second_csat_cume_dist,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type,
        t.event_at
    FROM outliers s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_at >= TIMESTAMP '2025-01-25 09:00:00'
)
SELECT
    id,
    agent_id,
    csat,
    csat_percent_rank,
    second_csat_cume_dist,
    minutes_since_prev_first_closed_at,
    second_first_closed_at,
    avg_second_csat_cume_dist,
    event_id,
    event_type,
    event_at,
    csat_percent_rank - second_csat_cume_dist AS csat_percent_rank_minus_second_csat_cume_dist,
    LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
FROM with_children
ORDER BY event_type, csat DESC, id, event_id
LIMIT 5
