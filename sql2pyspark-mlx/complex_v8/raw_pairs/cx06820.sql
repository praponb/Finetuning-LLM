WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        opened_at + INTERVAL 7 DAYS AS opened_at_plus_7d
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-02-04 23:00:00'
      AND priority = 'P1'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(opened_at) ORDER BY opened_at), 3) AS opened_at_cume_dist,
        LAST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY agent_id, csat ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_csat,
        ROUND((UNIX_TIMESTAMP(opened_at_plus_7d) - UNIX_TIMESTAMP(LAG(opened_at_plus_7d) OVER (PARTITION BY priority, channel ORDER BY opened_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_opened_at_plus_7d,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY YEAR(opened_at) ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        LAST_VALUE(opened_at_plus_7d) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY requester_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at_plus_7d,
        RANK() OVER (PARTITION BY agent_id ORDER BY requester_id NULLS LAST) AS requester_id_rank,
        ROUND(AVG(requester_id) OVER w_channel_priority, 2) AS channel_priority_avg_requester_id,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_6_prior,
        FIRST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat,
        NTH_VALUE(opened_at_plus_7d, 2) OVER (PARTITION BY agent_id ORDER BY requester_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at_plus_7d,
        ROUND(AVG(csat) OVER (PARTITION BY YEAR(opened_at)), 2) AS opened_at_year_avg_csat,
        MAX(requester_id) OVER (PARTITION BY priority, channel ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_requester_id_so_far
    FROM base
    WINDOW w_channel_priority AS (PARTITION BY channel, priority)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(first_known_csat) OVER (PARTITION BY priority) AS priority_max_first_known_csat,
        ROUND(opened_at_cume_dist / NULLIF(SUM(opened_at_cume_dist) OVER (PARTITION BY priority), 0), 4) AS opened_at_cume_dist_share_of_priority,
        ROUND(CUME_DIST() OVER (PARTITION BY first_known_csat, csat ORDER BY requester_id_rank DESC), 3) AS requester_id_rank_cume_dist,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel,
        ROUND(CUME_DIST() OVER (ORDER BY first_known_requester_id), 3) AS first_known_requester_id_cume_dist,
        first_known_requester_id - LAG(first_known_requester_id) OVER (ORDER BY max_requester_id_so_far DESC, id) AS first_known_requester_id_change
    FROM sequenced
)
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    channel,
    csat,
    opened_at_plus_7d,
    opened_at_cume_dist,
    first_known_requester_id,
    channel_priority_avg_requester_id,
    first_known_csat,
    opened_at_year_avg_csat,
    priority_max_first_known_csat,
    opened_at_cume_dist_share_of_priority,
    requester_id_rank_cume_dist,
    rows_in_channel,
    first_known_requester_id_change,
    LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
FROM with_trends
ORDER BY opened_at_cume_dist_share_of_priority, id
LIMIT 20
