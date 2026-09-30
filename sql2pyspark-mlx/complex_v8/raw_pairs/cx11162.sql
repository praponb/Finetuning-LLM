WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.requester_id) AS prior_requester_id_3d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(opened_at) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at,
        RANK() OVER (ORDER BY opened_at DESC, id) AS opened_at_rank,
        NTILE(10) OVER w_prior_rows_3d_opened_at_year AS prior_rows_3d_decile,
        SUM(prior_requester_id_3d) OVER (PARTITION BY TO_DATE(opened_at), agent_id) AS opened_at_to_date_agent_id_sum_prior_requester_i,
        ROUND(PERCENT_RANK() OVER (PARTITION BY prior_rows_3d, YEAR(opened_at) ORDER BY prior_rows_3d ASC, id), 3) AS prior_rows_3d_percent_rank
    FROM joined_ranges
    WINDOW w_prior_rows_3d_opened_at_year AS (PARTITION BY prior_rows_3d, YEAR(opened_at) ORDER BY prior_rows_3d ASC, id)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_windows
    WHERE prior_requester_id_3d > (SELECT AVG(prior_requester_id_3d) FROM with_windows)
),
with_activity_2 AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor = 'customer'
              AND HOUR(t.event_at) BETWEEN 10 AND 14
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, opened_at_rank, prior_requester_id_3d
    FROM with_activity_2
) src
UNPIVOT (
    metric_value FOR field IN (opened_at_rank AS opened_at_rank_v, prior_requester_id_3d AS m_prior_requester_id_3d)
) unp
