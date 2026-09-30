WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.requester_id) AS prior_requester_id_7d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
agent_id_stats AS (
    -- Median prior_requester_id_7d per agent_id
    SELECT agent_id, PERCENTILE_APPROX(prior_requester_id_7d, 0.5) AS median_prior_requester_id_7d
    FROM covered
    GROUP BY agent_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_requester_id_7d,
        ROUND(s.prior_requester_id_7d - g.median_prior_requester_id_7d, 2) AS prior_requester_id_7d_minus_median
    FROM covered s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.prior_requester_id_7d >= g.median_prior_requester_id_7d
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_7d,
        s.prior_requester_id_7d,
        s.median_prior_requester_id_7d,
        s.prior_requester_id_7d_minus_median,
        su.agent_name
    FROM above_average s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.team NOT IN ('Billing', 'Tech')
),
unique_rows AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        agent_id,
        opened_at,
        prior_rows_7d,
        prior_requester_id_7d,
        median_prior_requester_id_7d,
        prior_requester_id_7d_minus_median,
        agent_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY prior_requester_id_7d_minus_median DESC, id DESC) AS row_num
        FROM with_details
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    agent_id,
    opened_at,
    prior_rows_7d,
    median_prior_requester_id_7d,
    prior_requester_id_7d_minus_median,
    agent_name,
    TO_DATE(opened_at) AS opened_at_date,
    FLOOR(prior_requester_id_7d / 25) * 25 AS prior_requester_id_7d_bucket
FROM unique_rows
ORDER BY median_prior_requester_id_7d NULLS LAST
