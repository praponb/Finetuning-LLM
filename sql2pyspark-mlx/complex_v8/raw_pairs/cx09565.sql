WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        csat - requester_id AS csat_minus_requester_id,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND requester_id <= 14
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.channel,
        s.csat_minus_requester_id,
        su.tier AS agent_tier
    FROM cleaned_tickets s
    JOIN support_agents su ON su.id = s.agent_id AND UPPER(TRIM(su.agent_name)) LIKE 'J%'
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, MIN(csat) AS min_ticket_csat
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.min_ticket_csat
    FROM combined_details s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY agent_id, channel ORDER BY min_ticket_csat ASC, id) AS min_ticket_csat_quintile,
        MIN(csat_minus_requester_id) OVER (PARTITION BY agent_id) AS agent_id_min_csat_minus_requester_id,
        ROUND(csat_minus_requester_id / NULLIF(SUM(csat_minus_requester_id) OVER (PARTITION BY agent_id), 0), 4) AS csat_minus_requester_id_share_of_agent_id,
        ROUND(AVG(min_ticket_csat) OVER (PARTITION BY min_ticket_csat ORDER BY min_ticket_csat NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS min_ticket_csat_avg_3,
        MAX(min_ticket_csat) OVER (PARTITION BY min_ticket_csat ORDER BY min_ticket_csat NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS min_ticket_csat_max_4,
        RANK() OVER (PARTITION BY agent_id, channel ORDER BY min_ticket_csat ASC, id) AS min_ticket_csat_rank
    FROM profiled
)
-- Subtotals with GROUPING SETS
SELECT
    agent_tier,
    min_ticket_csat_rank,
    GROUPING(agent_tier) AS is_total_agent_tier,
    ARRAY_SORT(COLLECT_SET(channel)) AS channel_list
FROM windowed
GROUP BY GROUPING SETS ((agent_tier, min_ticket_csat_rank), (agent_tier), (min_ticket_csat_rank), ())
