WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 11:00:00'
      AND event_type <> 'reopened'
),
derived AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix
    FROM valid
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IS NOT NULL
        )
),
totals AS (
    -- Summarise per group
    SELECT
        ticket_id,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        SUM(CASE WHEN event_type NOT IN ('replied', 'resolved') THEN 1 ELSE 0 END) AS n_event_type_not_in_replied_re
    FROM with_activity
    WHERE HOUR(event_at) BETWEEN 9 AND 11
    GROUP BY ticket_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        ticket_id,
        n_rows,
        n_event_type_not_in_replied_re,
        SUM(n_rows) OVER (PARTITION BY ticket_id, n_event_type_not_in_replied_re ORDER BY distinct_event_types DESC NULLS FIRST, ticket_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS n_rows_sum_3_prior,
        MAX(n_rows) OVER (PARTITION BY ticket_id, n_event_type_not_in_replied_re) AS ticket_id_n_event_type_not_in_replied_re_max_n_r,
        NTH_VALUE(n_rows, 2) OVER (PARTITION BY ticket_id, n_event_type_not_in_replied_re ORDER BY distinct_event_types DESC NULLS FIRST, ticket_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_rows,
        ROUND(AVG(distinct_event_types) OVER (ORDER BY distinct_event_types DESC, ticket_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS distinct_event_types_avg_3_prior,
        ROUND(AVG(n_rows) OVER (PARTITION BY ticket_id, n_event_type_not_in_replied_re ORDER BY distinct_event_types DESC NULLS FIRST, ticket_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS n_rows_avg_6_prior,
        ROUND(AVG(n_rows) OVER (PARTITION BY ticket_id, n_event_type_not_in_replied_re), 2) AS ticket_id_n_event_type_not_in_replied_re_avg_n_r
    FROM totals
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.ticket_id,
        s.n_rows,
        s.n_event_type_not_in_replied_re,
        s.n_rows_sum_3_prior,
        s.ticket_id_n_event_type_not_in_replied_re_max_n_r,
        s.second_n_rows,
        s.distinct_event_types_avg_3_prior,
        s.n_rows_avg_6_prior,
        s.ticket_id_n_event_type_not_in_replied_re_avg_n_r,
        t.agent_id AS ticket_agent_id,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at
    FROM with_windows s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    ticket_id,
    n_rows,
    n_event_type_not_in_replied_re,
    n_rows_sum_3_prior,
    second_n_rows,
    n_rows_avg_6_prior,
    ticket_id_n_event_type_not_in_replied_re_avg_n_r,
    ticket_agent_id,
    ticket_closed_at,
    FLOOR(n_rows_sum_3_prior / 5) * 5 AS n_rows_sum_3_prior_bucket,
    ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(ticket_opened_at)) / 60, 1) AS minutes_ticket_opened_at_to_ticket_closed_at
FROM enriched_ticket
ORDER BY n_rows_avg_6_prior DESC
