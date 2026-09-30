WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ARRAY_CONTAINS(tags, 'refund') AS has_refund,
        MONTH(opened_at) AS opened_at_month,
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref
    FROM tickets
    WHERE agent_id >= 7
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM scoped_tickets s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND TO_DATE(t.event_at) < DATE '2025-02-12'
              AND t.actor IS NOT NULL
        )
),
screened_2 AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat < 3
              AND t.channel IS NOT NULL
        )
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(closed_at) AS closed_at_day,
        csat,
        channel,
        MIN(requester_id) AS min_requester_id,
        MIN(csat) AS min_csat,
        COUNT(*) AS cnt,
        SUM(csat) AS total_csat,
        MIN(opened_at) AS first_opened_at
    FROM screened_2
    WHERE opened_at_month > 3
    GROUP BY TO_DATE(closed_at), csat, channel
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(cnt, 3, 0) OVER w_channel_csat_by_min_requeste AS prev3_cnt,
        SIZE(COLLECT_SET(channel) OVER w_first_opened_at_month) AS distinct_channel_in_first_opened_at_month
    FROM rollup_base
    WINDOW w_channel_csat_by_min_requeste AS (PARTITION BY channel, csat ORDER BY min_requester_id ASC, closed_at_day, csat, channel), w_first_opened_at_month AS (PARTITION BY DATE_TRUNC('MONTH', first_opened_at))
),
screened_3 AS (
    -- Compare against the overall max
    SELECT *
    FROM windowed
    WHERE min_requester_id < (SELECT MAX(min_requester_id) FROM windowed)
)
SELECT
    closed_at_day,
    channel,
    min_requester_id,
    min_csat,
    total_csat,
    prev3_cnt,
    distinct_channel_in_first_opened_at_month,
    ABS(min_csat - 4) AS min_csat_distance,
    COALESCE(min_requester_id, 0) AS min_requester_id_filled
FROM screened_3
ORDER BY channel NULLS LAST
