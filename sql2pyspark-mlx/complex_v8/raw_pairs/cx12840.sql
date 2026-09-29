WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.requester_id) AS prior_requester_id_12h
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.agent_id, a.opened_at
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        QUARTER(opened_at) AS opened_at_quarter,
        DAYOFWEEK(opened_at) AS opened_at_dayofweek,
        FLOOR(prior_requester_id_12h / 25) * 25 AS prior_requester_id_12h_bucket
    FROM joined_ranges
    WHERE opened_at < TIMESTAMP '2025-02-24 08:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(opened_at_dayofweek) OVER (PARTITION BY opened_at_quarter ORDER BY opened_at_quarter DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS opened_at_dayofweek_avg_2,
        prior_rows_12h - LAG(prior_rows_12h) OVER (PARTITION BY opened_at_quarter ORDER BY opened_at_quarter DESC, id) AS prior_rows_12h_change,
        FIRST_VALUE(opened_at) OVER (ORDER BY opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at,
        LEAD(opened_at_dayofweek) OVER (PARTITION BY opened_at_quarter ORDER BY opened_at_quarter DESC, id) AS next_opened_at_dayofweek,
        LEAD(prior_requester_id_12h) OVER (PARTITION BY opened_at_quarter ORDER BY opened_at_quarter DESC, id) AS next_prior_requester_id_12h
    FROM annotated
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM metrics
    WHERE next_prior_requester_id_12h < (SELECT MAX(next_prior_requester_id_12h) FROM metrics)
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    prior_requester_id_12h,
    opened_at_dayofweek,
    opened_at_dayofweek_avg_2,
    prior_rows_12h_change,
    first_opened_at,
    next_opened_at_dayofweek
FROM qualified
