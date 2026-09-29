WITH in_window AS (
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
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_1d,
        s.prior_requester_id_1d,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM in_window s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        opened_at,
        prior_rows_1d,
        agent_hired_on,
        FLOOR(prior_requester_id_1d / 25) * 25 AS prior_requester_id_1d_bucket,
        SUBSTRING(TRIM(agent_name), 1, 2) AS agent_name_prefix
    FROM enriched
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_id,
        ROUND(STDDEV(prior_rows_1d), 2) AS stddev_prior_rows_1d,
        MIN(agent_hired_on) AS first_agent_hired_on,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        MAX(prior_rows_1d) AS max_prior_rows_1d
    FROM annotated
    GROUP BY agent_id
    HAVING MAX(prior_rows_1d) > 1
)
SELECT
    max_prior_rows_1d,
    agent_id,
    GROUPING_ID() AS grouping_level,
    ROUND(AVG(stddev_prior_rows_1d), 2) AS avg_stddev_prior_rows_1d,
    COUNT(*) AS cnt,
    SUM(max_prior_rows_1d) AS total_max_prior_rows_1d
FROM per_group
GROUP BY ROLLUP(max_prior_rows_1d, agent_id)
