WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
        csat - requester_id AS csat_minus_requester_id
    FROM tickets
    WHERE (agent_id >= 3 OR channel IS NOT NULL)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        opened_at,
        channel,
        tags,
        channel_slug,
        csat_minus_requester_id,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        DENSE_RANK() OVER (PARTITION BY agent_id ORDER BY csat DESC, id) AS csat_dense_rank,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        SUM(csat_minus_requester_id) OVER (PARTITION BY csat, channel ORDER BY csat_minus_requester_id DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS csat_minus_requester_id_sum_2_prior,
        MAX(requester_id) OVER (PARTITION BY agent_id ORDER BY csat DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS requester_id_max_1_prior,
        MIN(requester_id) OVER (PARTITION BY agent_id, csat ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id, csat ORDER BY requester_id DESC), 3) AS requester_id_cume_dist,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (ORDER BY opened_at DESC, id))) / 60, 1) AS minutes_since_prev_opened_at,
        MAX(requester_id) OVER (PARTITION BY csat, TO_DATE(opened_at)) AS csat_opened_at_to_date_max_requester_id,
        ROUND(CUME_DIST() OVER (ORDER BY opened_at DESC), 3) AS opened_at_cume_dist,
        DENSE_RANK() OVER w_agent_id_csat_by_requester_i_2 AS requester_id_dense_rank
    FROM base_tickets
    WINDOW w_agent_id_csat_by_requester_i_2 AS (PARTITION BY agent_id, csat ORDER BY requester_id DESC)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        opened_at,
        tags,
        channel_slug,
        csat_minus_requester_id,
        last_known_closed_at,
        last_known_opened_at,
        requester_id_max_1_prior,
        min_requester_id_so_far,
        requester_id_cume_dist,
        csat_opened_at_to_date_max_requester_id,
        opened_at_cume_dist,
        LAST_VALUE(last_known_opened_at) OVER (PARTITION BY channel, MONTH(last_known_closed_at) ORDER BY last_known_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_known_opened_at,
        SUM(min_requester_id_so_far) OVER (PARTITION BY csat_dense_rank ORDER BY csat_minus_requester_id_sum_2_prior ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS min_requester_id_so_far_sum_7,
        ROUND(AVG(csat_dense_rank) OVER (PARTITION BY csat_dense_rank ORDER BY csat_minus_requester_id_sum_2_prior ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS csat_dense_rank_avg_3,
        ROUND(csat_minus_requester_id / NULLIF(SUM(csat_minus_requester_id) OVER w_channel, 0), 4) AS csat_minus_requester_id_share_of_channel,
        NTILE(5) OVER w_channel_last_known_closed_at_2 AS last_known_opened_at_quintile
    FROM with_windows
    WINDOW w_channel AS (PARTITION BY channel), w_channel_last_known_closed_at_2 AS (PARTITION BY channel, MONTH(last_known_closed_at) ORDER BY last_known_opened_at DESC, id)
)
-- Final projection
SELECT
    id,
    requester_id,
    opened_at,
    tags,
    channel_slug,
    csat_minus_requester_id,
    last_known_closed_at,
    last_known_opened_at,
    requester_id_max_1_prior,
    min_requester_id_so_far,
    requester_id_cume_dist,
    csat_opened_at_to_date_max_requester_id,
    opened_at_cume_dist,
    min_requester_id_so_far_sum_7,
    csat_dense_rank_avg_3,
    csat_minus_requester_id_share_of_channel,
    last_known_opened_at_quintile,
    requester_id_max_1_prior - csat_minus_requester_id AS requester_id_max_1_prior_minus_csat_minus_reques,
    DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at
FROM windowed
WHERE opened_at_cume_dist > 0.1
ORDER BY last_known_opened_at, id
LIMIT 10
