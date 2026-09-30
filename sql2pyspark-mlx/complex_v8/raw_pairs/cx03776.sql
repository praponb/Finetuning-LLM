WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix,
        HOUR(closed_at) AS closed_at_hour
    FROM tickets
    WHERE requester_id < 23
      AND priority = 'P1'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        csat,
        tags,
        channel_ref,
        channel_prefix,
        closed_at_hour,
        NTH_VALUE(closed_at_hour, 2) OVER (PARTITION BY csat ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at_hour,
        NTILE(10) OVER (PARTITION BY csat ORDER BY csat, id) AS csat_decile,
        ROUND(csat - AVG(csat) OVER (PARTITION BY priority), 2) AS csat_vs_priority_avg,
        NTH_VALUE(csat, 2) OVER (PARTITION BY csat ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        MIN(closed_at_hour) OVER w_priority AS priority_min_closed_at_hour
    FROM cleaned_tickets
    WINDOW w_priority AS (PARTITION BY priority)
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.channel,
        s.csat,
        s.tags,
        s.channel_ref,
        s.channel_prefix,
        s.csat_decile,
        s.csat_vs_priority_avg,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type
    FROM with_windows s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    csat,
    channel_ref,
    csat_decile,
    csat_vs_priority_avg,
    event_id,
    event_actor,
    SIZE(tags) AS tags_count,
    ROUND(csat_vs_priority_avg / NULLIF(csat_decile, 0), 3) AS csat_vs_priority_avg_per_csat_decile
FROM detail_rows
ORDER BY csat, csat_decile DESC
