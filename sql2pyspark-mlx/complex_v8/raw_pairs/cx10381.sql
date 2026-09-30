WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        DAYOFWEEK(closed_at) AS closed_at_dayofweek,
        TO_DATE(closed_at) AS closed_at_date
    FROM tickets
    WHERE csat >= 2
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM filtered_tickets s
    WHERE s.id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.actor = 'agent'
              AND t.event_type IN ('resolved', 'replied', 'escalated')
        )
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        csat_per_requester_id,
        closed_at_dayofweek,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel, YEAR(closed_at_date))) AS distinct_agent_id_in_channel_closed_at_date_year,
        FIRST_VALUE(closed_at_dayofweek) IGNORE NULLS OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at_dayofweek,
        COUNT(*) OVER (PARTITION BY channel, YEAR(closed_at_date)) AS rows_in_channel_closed_at_date_year,
        ROUND(csat_per_requester_id / NULLIF(SUM(csat_per_requester_id) OVER (PARTITION BY channel, YEAR(closed_at_date)), 0), 4) AS csat_per_requester_id_share_of_channel_closed_at,
        MAX(csat_per_requester_id) OVER (PARTITION BY MONTH(opened_at) ORDER BY opened_at ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS csat_per_requester_id_max_2
    FROM with_activity
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, requester_id, first_known_closed_at_dayofweek, distinct_agent_id_in_channel_closed_at_date_year, rows_in_channel_closed_at_date_year
    FROM with_trends
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (requester_id, first_known_closed_at_dayofweek, distinct_agent_id_in_channel_closed_at_date_year, rows_in_channel_closed_at_date_year)
) unp
