WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        DAYOFWEEK(closed_at) AS closed_at_dayofweek
    FROM tickets
    WHERE requester_id < 18
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        closed_at,
        priority,
        channel,
        closed_at_dayofweek,
        LAST_VALUE(closed_at_dayofweek) IGNORE NULLS OVER (ORDER BY closed_at_dayofweek DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at_dayofweek,
        NTH_VALUE(requester_id, 2) OVER (ORDER BY closed_at_dayofweek DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        LAST_VALUE(closed_at_dayofweek) IGNORE NULLS OVER (PARTITION BY priority, agent_id ORDER BY requester_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at_dayofweek_2,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (PARTITION BY priority, agent_id ORDER BY requester_id DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_closed_at,
        COUNT(*) OVER (PARTITION BY closed_at_dayofweek, channel) AS rows_in_closed_at_dayofweek_channel,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY channel ORDER BY closed_at_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id_2,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY closed_at_dayofweek)) AS distinct_priority_in_closed_at_dayofweek,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY closed_at_dayofweek), 0), 4) AS csat_share_of_closed_at_dayofweek,
        LAG(requester_id, 2) OVER (PARTITION BY priority, agent_id ORDER BY requester_id DESC NULLS FIRST, id) AS prev2_requester_id,
        DENSE_RANK() OVER (ORDER BY closed_at_dayofweek) AS closed_at_dayofweek_dense_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_closed_at_dayofweek, 3) AS closed_at_dayofweek_percent_rank,
        LEAD(closed_at_dayofweek, 3) OVER (PARTITION BY priority, agent_id ORDER BY requester_id DESC NULLS FIRST, id) AS next3_closed_at_dayofweek
    FROM recent_tickets
    WINDOW w_all_by_closed_at_dayofweek AS (ORDER BY closed_at_dayofweek DESC, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority, closed_at_dayofweek ORDER BY second_requester_id), 3) AS second_requester_id_percent_rank,
        ROUND(prev2_requester_id / NULLIF(SUM(prev2_requester_id) OVER (PARTITION BY distinct_priority_in_closed_at_dayofweek), 0), 4) AS prev2_requester_id_share_of_distinct_priority_in,
        MIN(closed_at_dayofweek) OVER (PARTITION BY distinct_priority_in_closed_at_dayofweek) AS distinct_priority_in_closed_at_dayofweek_min_clo,
        COUNT(*) OVER (PARTITION BY distinct_priority_in_closed_at_dayofweek) AS rows_in_distinct_priority_in_closed_at_dayofweek
    FROM metrics
)
SELECT
    id,
    requester_id,
    closed_at,
    priority,
    channel,
    closed_at_dayofweek,
    last_known_closed_at_dayofweek,
    rows_in_closed_at_dayofweek_channel,
    second_requester_id_2,
    distinct_priority_in_closed_at_dayofweek,
    prev2_requester_id,
    closed_at_dayofweek_dense_rank,
    closed_at_dayofweek_percent_rank,
    second_requester_id_percent_rank,
    prev2_requester_id_share_of_distinct_priority_in,
    distinct_priority_in_closed_at_dayofweek_min_clo,
    rows_in_distinct_priority_in_closed_at_dayofweek
FROM windowed
ORDER BY prev2_requester_id DESC
