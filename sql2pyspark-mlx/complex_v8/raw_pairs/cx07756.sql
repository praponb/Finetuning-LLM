WITH covered AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_ym,
        agent_id,
        COUNT(*) AS cnt,
        MIN(event_at) AS first_event_at
    FROM covered
    WHERE agent_id IS NOT NULL
    GROUP BY DATE_FORMAT(event_at, 'yyyy-MM'), agent_id
    HAVING SUM(hours_since_open) > 21.6
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        event_at_ym,
        agent_id,
        cnt,
        first_event_at,
        ROW_NUMBER() OVER (ORDER BY cnt, event_at_ym, agent_id) AS rn,
        SUM(cnt) OVER (ORDER BY cnt, event_at_ym, agent_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cnt_sum_2_prior,
        RANK() OVER (ORDER BY cnt) AS cnt_rank
    FROM grouped
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(cnt_sum_2_prior - 20) AS cnt_sum_2_prior_distance,
        DATE_FORMAT(first_event_at, 'MMM yyyy') AS first_event_at_label
    FROM ranked
    WHERE event_at_ym IS NOT NULL
)
SELECT
    event_at_ym,
    first_event_at,
    cnt_sum_2_prior,
    cnt_rank,
    cnt_sum_2_prior_distance,
    first_event_at_label
FROM annotated
ORDER BY event_at_ym, first_event_at_label
