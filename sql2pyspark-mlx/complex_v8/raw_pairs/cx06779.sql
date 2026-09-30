WITH valid_tickets AS (
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
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
    FROM tickets
    WHERE requester_id <= 22
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS('|', tags_text, priority) AS tags_text_priority_label,
        DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_label
    FROM valid_tickets
),
enriched_2 AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(csat, 0) AS csat_filled,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM enriched
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.csat,
        s.tags_text,
        s.closed_at_label,
        s.csat_filled,
        s.requester_id_per_csat,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor
    FROM enriched_2 s
    JOIN ticket_events t ON t.ticket_id = s.id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        channel,
        tags_text,
        closed_at_label,
        csat_filled,
        requester_id_per_csat,
        event_id,
        event_at,
        ROUND(AVG(csat) OVER w_event_actor_csat, 2) AS event_actor_csat_avg_csat,
        LAST_VALUE(csat_filled) IGNORE NULLS OVER (ORDER BY csat ASC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_csat_filled,
        SUM(requester_id_per_csat) OVER (ORDER BY csat ASC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id_per_csat,
        ROW_NUMBER() OVER (ORDER BY event_at ASC, id, event_id) AS rn,
        LAG(event_at) OVER w_all_by_event_at AS prev_event_at,
        SUM(csat_filled) OVER (ORDER BY event_at ASC, id, event_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS csat_filled_sum_1_prior
    FROM line_level
    WINDOW w_event_actor_csat AS (PARTITION BY event_actor, csat), w_all_by_event_at AS (ORDER BY event_at ASC, id, event_id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_known_csat_filled, channel, csat_filled
    FROM with_trends
) src
PIVOT (
    SUM(csat_filled)
    FOR channel IN ('chat', 'phone')
)
