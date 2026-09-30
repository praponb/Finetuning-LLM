WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at,
        ABS(csat - 4) AS csat_distance,
        CONCAT(channel, '#', CAST(csat AS STRING)) AS channel_ref
    FROM tickets
    WHERE requester_id < 18
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY channel, TO_DATE(opened_at) ORDER BY csat DESC, id), 3) AS csat_cume_dist,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY channel, TO_DATE(opened_at) ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        NTH_VALUE(csat_distance, 2) OVER (PARTITION BY channel, TO_DATE(opened_at) ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat_distance,
        ROUND(requester_id - AVG(requester_id) OVER w_agent_id, 2) AS requester_id_vs_agent_id_avg
    FROM base_tickets
    WINDOW w_agent_id AS (PARTITION BY agent_id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(second_closed_at) OVER (PARTITION BY TO_DATE(opened_at), agent_id ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_second_closed_at,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat), 0), 4) AS csat_share_of_csat,
        DENSE_RANK() OVER w_opened_at_to_date_agent_id_b_2 AS closed_at_dense_rank,
        LEAD(csat_cume_dist, 3) OVER w_opened_at_to_date_agent_id_b_2 AS next3_csat_cume_dist,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY csat)) AS distinct_agent_id_in_csat
    FROM sequenced
    WINDOW w_opened_at_to_date_agent_id_b_2 AS (PARTITION BY TO_DATE(opened_at), agent_id ORDER BY closed_at DESC, id)
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.tags,
        s.minutes_opened_at_to_closed_at,
        s.csat_cume_dist,
        s.second_closed_at,
        s.requester_id_vs_agent_id_avg,
        s.first_second_closed_at,
        s.closed_at_dense_rank,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type,
        t.event_at
    FROM windowed s
    JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT *
FROM (
    SELECT event_at, event_id, event_type, requester_id_vs_agent_id_avg
    FROM with_children
) src
PIVOT (
    COUNT(requester_id_vs_agent_id_avg)
    FOR event_type IN ('created', 'escalated', 'resolved')
)
