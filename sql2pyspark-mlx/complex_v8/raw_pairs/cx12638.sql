WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(requester_id) OVER (PARTITION BY agent_id ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS requester_id_sum_1_prior,
        SUM(csat) OVER w_csat_closed_at_month AS csat_closed_at_month_sum_csat,
        ROUND(csat - AVG(csat) OVER (PARTITION BY csat, MONTH(closed_at)), 2) AS csat_vs_csat_closed_at_month_avg
    FROM scoped
    WINDOW w_csat_closed_at_month AS (PARTITION BY csat, MONTH(closed_at))
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        closed_at,
        channel,
        csat,
        requester_id_sum_1_prior,
        csat_closed_at_month_sum_csat,
        csat_vs_csat_closed_at_month_avg,
        ABS(csat - 4) AS csat_distance,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd') AS closed_at_label,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd') AS closed_at_label_2
    FROM with_windows
    WHERE requester_id_sum_1_prior >= 12
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
        )
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM without_matches s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND HOUR(t.event_at) BETWEEN 10 AND 14
        )
)
SELECT *
FROM (
    SELECT csat, csat_distance, channel, csat_vs_csat_closed_at_month_avg
    FROM screened
) src
PIVOT (
    SUM(csat_vs_csat_closed_at_month_avg)
    FOR channel IN ('email', 'phone', 'web', 'chat')
)
ORDER BY csat, csat_distance
