WITH base AS (
    -- Keep valid rows and derive helper columns
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
        SUBSTRING(TRIM(priority), 1, 4) AS priority_prefix,
        SIZE(tags) AS tags_count,
        CASE WHEN channel IN ('chat', 'phone') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        csat,
        priority_prefix,
        tags_count,
        channel_group,
        SUM(csat) OVER (PARTITION BY priority) AS priority_sum_csat,
        ROUND(tags_count - AVG(tags_count) OVER w_priority, 2) AS tags_count_vs_priority_avg,
        RANK() OVER (ORDER BY opened_at ASC) AS opened_at_rank
    FROM base
    WINDOW w_priority AS (PARTITION BY priority)
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.opened_at,
        s.closed_at,
        s.csat,
        s.priority_prefix,
        s.priority_sum_csat,
        s.tags_count_vs_priority_avg,
        s.opened_at_rank,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at,
        t.event_type
    FROM sequenced s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
enriched AS (
    -- Attach event attributes
    SELECT
        s.id,
        s.closed_at,
        s.csat,
        s.priority_prefix,
        s.priority_sum_csat,
        s.tags_count_vs_priority_avg,
        s.event_id,
        s.event_actor,
        s.event_at,
        s.event_type,
        t.ticket_id AS event_ticket_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor_2,
        ti.closed_at AS ticket_closed_at
    FROM detail_rows s
    LEFT JOIN ticket_events t ON t.id = s.event_id
    LEFT JOIN tickets ti ON ti.id = t.ticket_id
),
deduped AS (
    -- Keep the latest row per event_actor
    SELECT
        id,
        closed_at,
        csat,
        priority_prefix,
        priority_sum_csat,
        tags_count_vs_priority_avg,
        event_id,
        event_actor,
        event_at,
        event_type,
        event_ticket_id,
        event_event_type,
        event_actor_2,
        ticket_closed_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_actor ORDER BY tags_count_vs_priority_avg DESC, id DESC, event_id DESC) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(event_at) OVER w_ticket_closed_at_year_closed AS prev_event_at,
        MAX(csat) OVER (ORDER BY csat DESC, id, event_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS csat_max_3,
        MAX(tags_count_vs_priority_avg) OVER (PARTITION BY event_event_type ORDER BY closed_at ASC, id, event_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS tags_count_vs_priority_avg_max_1_prior,
        ROUND(AVG(tags_count_vs_priority_avg) OVER (ORDER BY csat DESC, id, event_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS tags_count_vs_priority_avg_avg_4
    FROM deduped
    WINDOW w_ticket_closed_at_year_closed AS (PARTITION BY YEAR(ticket_closed_at), YEAR(closed_at) ORDER BY event_at ASC, id, event_id)
)
-- Subtotals with GROUPING SETS
SELECT
    event_actor,
    event_event_type,
    COUNT(*) AS n_rows
FROM with_trends
GROUP BY GROUPING SETS ((event_actor, event_event_type), (event_actor), (event_event_type))
ORDER BY event_actor NULLS LAST, event_event_type NULLS LAST
