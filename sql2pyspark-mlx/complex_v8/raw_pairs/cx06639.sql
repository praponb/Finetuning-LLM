WITH matched AS (
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
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY agent_id ORDER BY prior_requester_id_12h ASC) AS prior_requester_id_12h_rank,
        prior_rows_12h - LAG(prior_rows_12h) OVER (PARTITION BY prior_rows_12h ORDER BY opened_at DESC, id) AS prior_rows_12h_change,
        MAX(prior_rows_12h) OVER (PARTITION BY prior_rows_12h ORDER BY opened_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prior_rows_12h_max_7,
        ROUND(prior_rows_12h / NULLIF(SUM(prior_rows_12h) OVER (PARTITION BY agent_id), 0), 4) AS prior_rows_12h_share_of_agent_id,
        COUNT(*) OVER (PARTITION BY prior_rows_12h ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM matched
)
SELECT
    id,
    agent_id,
    opened_at,
    prior_rows_12h,
    prior_requester_id_12h_rank,
    prior_rows_12h_change,
    prior_rows_12h_max_7,
    prior_rows_12h_share_of_agent_id
FROM sequenced
