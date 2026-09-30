WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority
    FROM tickets
    WHERE SIZE(tags) >= 1
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM scoped s
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM profiled s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
        )
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        opened_at,
        priority,
        CASE WHEN priority IN ('P3', 'P2') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group,
        CASE WHEN ticket_count >= 6 THEN 'large' WHEN ticket_count >= 2 THEN 'medium' ELSE 'small' END AS ticket_count_tier,
        CASE WHEN ticket_count >= 4 THEN 'HIGH' WHEN ticket_count >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS ticket_count_tier_2
    FROM filtered
    WHERE opened_at >= TIMESTAMP '2025-01-07 13:00:00'
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.opened_at,
        s.priority,
        s.priority_group,
        s.ticket_count_tier_2,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at
    FROM scored s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_at,
        priority_group,
        ticket_count_tier_2,
        event_id,
        event_actor,
        event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_count_tier_2 ORDER BY opened_at ASC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROW_NUMBER() OVER w_ticket_count_tier_2_by_opene_2 AS opened_at_seq,
        LAG(opened_at) OVER w_ticket_count_tier_2_by_opene_2 AS prev_opened_at,
        LAG(opened_at) OVER (ORDER BY opened_at, id, event_id) AS prev_opened_at_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_count_tier_2 ORDER BY opened_at ASC, id, event_id), 3) AS opened_at_percent_rank
    FROM with_children
    WINDOW w_ticket_count_tier_2_by_opene_2 AS (PARTITION BY ticket_count_tier_2 ORDER BY opened_at ASC, id, event_id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(last_known_event_at) - UNIX_TIMESTAMP(LAG(last_known_event_at) OVER w_all_by_opened_at_seq)) / 60, 1) AS minutes_since_prev_last_known_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_count_tier_2, event_actor ORDER BY last_known_event_at ASC, id, event_id), 3) AS last_known_event_at_percent_rank,
        NTILE(3) OVER w_ticket_count_tier_2_event_ac AS last_known_event_at_tercile,
        COUNT(*) OVER (PARTITION BY MONTH(prev_opened_at), priority_group) AS rows_in_prev_opened_at_month_priority_group
    FROM ranked
    WINDOW w_all_by_opened_at_seq AS (ORDER BY opened_at_seq, id, event_id), w_ticket_count_tier_2_event_ac AS (PARTITION BY ticket_count_tier_2, event_actor ORDER BY last_known_event_at ASC, id, event_id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, event_id, measure, reading
FROM (
    SELECT id, event_id, opened_at_percent_rank, last_known_event_at_percent_rank, minutes_since_prev_last_known_event_at
    FROM metrics
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (opened_at_percent_rank AS m_opened_at_percent_rank, last_known_event_at_percent_rank AS m_last_known_event_at_percent_rank, minutes_since_prev_last_known_event_at AS minutes_since_prev_last_known_event_at_v)
) unp
