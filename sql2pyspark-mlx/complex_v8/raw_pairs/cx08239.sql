WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_type IN ('assigned', 'replied')
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.actor_or_unknown,
        t.csat AS ticket_csat,
        t.requester_id AS ticket_requester_id,
        t.closed_at AS ticket_closed_at
    FROM recent s
    JOIN tickets t ON t.id = s.ticket_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(ticket_closed_at, 'MMM yyyy') AS ticket_closed_at_label,
        DATEDIFF(DATE '2026-01-01', ticket_closed_at) AS days_since_ticket_closed_at,
        CONCAT_WS('|', actor, actor_or_unknown) AS actor_actor_or_unknown_label
    FROM with_details_ticket
    WHERE MONTH(ticket_closed_at) IN (1, 2)
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM annotated s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
totals AS (
    -- Summarise per group
    SELECT
        event_count,
        ticket_id,
        ROUND(SUM(ticket_csat) / NULLIF(SUM(ticket_requester_id), 0), 3) AS ticket_csat_to_ticket_requester_id_ratio,
        PERCENTILE_APPROX(event_count, 0.5) AS median_event_count,
        MIN(ticket_csat) AS min_ticket_csat,
        COUNT(*) AS cnt
    FROM with_counts
    GROUP BY 1, 2
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        event_count,
        ticket_id,
        ticket_csat_to_ticket_requester_id_ratio,
        min_ticket_csat,
        SUM(cnt) OVER (PARTITION BY cnt, min_ticket_csat ORDER BY min_ticket_csat ASC, event_count, ticket_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cnt,
        ROUND(AVG(event_count) OVER (PARTITION BY cnt, min_ticket_csat ORDER BY min_ticket_csat ASC, event_count, ticket_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS event_count_avg_4,
        COUNT(*) OVER (PARTITION BY event_count) AS rows_in_event_count,
        ROUND(event_count / NULLIF(SUM(event_count) OVER w_event_count, 0), 4) AS event_count_share_of_event_count,
        NTH_VALUE(median_event_count, 2) OVER (PARTITION BY cnt, min_ticket_csat ORDER BY min_ticket_csat ASC, event_count, ticket_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_median_event_count,
        ROUND(PERCENT_RANK() OVER (ORDER BY min_ticket_csat ASC), 3) AS min_ticket_csat_percent_rank
    FROM totals
    WINDOW w_event_count AS (PARTITION BY event_count)
)
SELECT
    event_count,
    ticket_csat_to_ticket_requester_id_ratio,
    min_ticket_csat,
    running_cnt,
    event_count_avg_4,
    rows_in_event_count,
    event_count_share_of_event_count,
    second_median_event_count,
    min_ticket_csat_percent_rank,
    GREATEST(running_cnt, event_count) AS greatest_running_cnt_event_count
FROM sequenced
WHERE rows_in_event_count <= 15
ORDER BY rows_in_event_count, running_cnt DESC
