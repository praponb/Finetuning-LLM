WITH in_window AS (
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
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_id,
        prior_rows_12h,
        MIN(opened_at) AS first_opened_at,
        COUNT(*) AS cnt,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        SUM(prior_requester_id_12h) AS total_prior_requester_id_12h,
        ROUND(SUM(prior_rows_12h) / NULLIF(SUM(prior_requester_id_12h), 0), 3) AS prior_rows_12h_to_prior_requester_id_12h_ratio,
        ROUND(STDDEV(prior_requester_id_12h), 2) AS stddev_prior_requester_id_12h
    FROM in_window
    GROUP BY agent_id, prior_rows_12h
    HAVING AVG(prior_rows_12h) > 3
)
-- Final projection
SELECT
    agent_id,
    prior_rows_12h,
    first_opened_at,
    distinct_agent_ids,
    total_prior_requester_id_12h,
    stddev_prior_requester_id_12h,
    COALESCE(cnt, 0) AS cnt_filled
FROM grouped
WHERE prior_rows_12h > 1
ORDER BY total_prior_requester_id_12h NULLS LAST
