WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.requester_id) AS prior_requester_id_1d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_prior_rows_1d_opened_at_mont AS rows_in_prior_rows_1d_opened_at_month,
        SUM(prior_requester_id_1d) OVER (PARTITION BY prior_rows_1d, MONTH(opened_at)) AS prior_rows_1d_opened_at_month_sum_prior_requeste
    FROM joined_ranges
    WINDOW w_prior_rows_1d_opened_at_mont AS (PARTITION BY prior_rows_1d, MONTH(opened_at))
)
SELECT
    id,
    prior_rows_1d,
    prior_requester_id_1d,
    rows_in_prior_rows_1d_opened_at_month,
    COALESCE(prior_rows_1d, 0) AS prior_rows_1d_filled
FROM with_trends
ORDER BY id DESC
LIMIT 5
