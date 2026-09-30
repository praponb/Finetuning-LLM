WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        tags,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM tickets
    WHERE csat < 5
      AND requester_id BETWEEN 7 AND 23
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        tags,
        requester_id_per_csat,
        ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY requester_id DESC, id) AS requester_id_seq,
        LEAD(requester_id) OVER (PARTITION BY agent_id ORDER BY requester_id_per_csat DESC, id) AS next_requester_id,
        requester_id - LAG(requester_id) OVER (ORDER BY requester_id DESC NULLS FIRST, id) AS requester_id_change,
        MIN(requester_id_per_csat) OVER (PARTITION BY channel, agent_id) AS channel_agent_id_min_requester_id_per_csat,
        MAX(requester_id) OVER (PARTITION BY channel, agent_id) AS channel_agent_id_max_requester_id,
        NTH_VALUE(requester_id_per_csat, 2) OVER (PARTITION BY channel ORDER BY requester_id_per_csat ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id_per_csat,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id ORDER BY requester_id_per_csat DESC, id), 3) AS requester_id_per_csat_cume_dist,
        SUM(requester_id) OVER (PARTITION BY agent_id, channel ORDER BY requester_id DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS requester_id_sum_2_prior,
        NTH_VALUE(requester_id_per_csat, 2) OVER (ORDER BY requester_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id_per_csat_2
    FROM scoped
    WINDOW w_channel_agent_id AS (PARTITION BY channel, agent_id)
)
SELECT
    id,
    channel,
    requester_id_per_csat,
    next_requester_id,
    requester_id_change,
    channel_agent_id_min_requester_id_per_csat,
    second_requester_id_per_csat,
    requester_id_per_csat_cume_dist,
    requester_id_sum_2_prior,
    second_requester_id_per_csat_2,
    CASE WHEN channel IN ('chat', 'email') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group,
    ROUND(requester_id_sum_2_prior / NULLIF(next_requester_id, 0), 3) AS requester_id_sum_2_prior_per_next_requester_id
FROM sequenced
WHERE requester_id_per_csat_cume_dist BETWEEN 0.2 AND 0.7
ORDER BY channel_agent_id_min_requester_id_per_csat DESC, id DESC NULLS FIRST
