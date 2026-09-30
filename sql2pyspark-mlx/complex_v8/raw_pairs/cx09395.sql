WITH valid_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE csat >= 3
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(requester_id - AVG(requester_id) OVER w_csat, 2) AS requester_id_vs_csat_avg,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY csat ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY csat)) AS distinct_agent_id_in_csat,
        DENSE_RANK() OVER (PARTITION BY csat ORDER BY requester_id DESC) AS requester_id_dense_rank,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_csat, 0), 4) AS requester_id_share_of_csat,
        NTILE(10) OVER (PARTITION BY csat ORDER BY requester_id DESC, id) AS requester_id_decile,
        COUNT(*) OVER (PARTITION BY csat) AS rows_in_csat,
        MAX(requester_id) OVER (PARTITION BY agent_id ORDER BY requester_id ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS requester_id_max_6_prior,
        LAG(requester_id, 3, 0) OVER (PARTITION BY csat ORDER BY requester_id DESC, id) AS prev3_requester_id,
        LEAD(closed_at, 3) OVER (PARTITION BY csat ORDER BY requester_id, id) AS next3_closed_at,
        LAG(csat, 2) OVER (PARTITION BY agent_id ORDER BY requester_id ASC NULLS LAST, id) AS prev2_csat
    FROM valid_tickets
    WINDOW w_csat AS (PARTITION BY csat)
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(requester_id_share_of_csat) OVER (PARTITION BY MONTH(closed_at), YEAR(next3_closed_at) ORDER BY prev3_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id_share_of_csat,
        LAG(prev3_requester_id) OVER (PARTITION BY MONTH(closed_at), YEAR(next3_closed_at) ORDER BY prev3_requester_id DESC, id) AS prev_prev3_requester_id,
        DENSE_RANK() OVER w_next3_closed_at_month_agent AS prev2_csat_dense_rank,
        MAX(requester_id_max_6_prior) OVER (PARTITION BY distinct_agent_id_in_csat ORDER BY requester_id, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS requester_id_max_6_prior_max_6_prior
    FROM ranked
    WINDOW w_next3_closed_at_month_agent AS (PARTITION BY DATE_TRUNC('MONTH', next3_closed_at), agent_id ORDER BY prev2_csat DESC)
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    csat,
    channel_slug,
    second_requester_id,
    distinct_agent_id_in_csat,
    requester_id_dense_rank,
    requester_id_share_of_csat,
    requester_id_decile,
    requester_id_max_6_prior,
    prev3_requester_id,
    next3_closed_at,
    prev2_csat,
    last_requester_id_share_of_csat,
    prev_prev3_requester_id,
    prev2_csat_dense_rank,
    requester_id_max_6_prior_max_6_prior
FROM ranked_2
WHERE channel_slug IS NOT NULL
ORDER BY requester_id_decile DESC, requester_id_max_6_prior
