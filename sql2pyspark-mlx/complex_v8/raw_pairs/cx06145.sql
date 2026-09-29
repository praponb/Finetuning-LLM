WITH filtered_tickets AS (
    -- Filter tickets
    SELECT *
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-24 09:00:00'
      AND priority = 'P3'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        MAX(csat) OVER (PARTITION BY csat, channel) AS csat_channel_max_csat,
        LEAD(requester_id) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS next_requester_id,
        DENSE_RANK() OVER w_closed_at_month_by_closed_at AS closed_at_dense_rank,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY csat, channel), 0), 4) AS requester_id_share_of_csat_channel,
        csat - LAG(csat) OVER (PARTITION BY channel, TO_DATE(closed_at) ORDER BY opened_at ASC, id) AS csat_change,
        COUNT(*) OVER (PARTITION BY channel, agent_id) AS rows_in_channel_agent_id,
        MAX(csat) OVER (PARTITION BY agent_id, MONTH(closed_at)) AS agent_id_closed_at_month_max_csat,
        SUM(csat) OVER (PARTITION BY channel, TO_DATE(closed_at) ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        MIN(csat) OVER (PARTITION BY csat, channel) AS csat_channel_min_csat,
        MIN(requester_id) OVER (PARTITION BY channel, TO_DATE(closed_at) ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        RANK() OVER w_channel_closed_at_to_date_by_3 AS opened_at_rank,
        NTILE(10) OVER w_agent_id_by_opened_at AS opened_at_decile,
        RANK() OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS opened_at_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id ORDER BY opened_at, id), 3) AS opened_at_cume_dist,
        LAST_VALUE(closed_at) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at,
        ROUND(csat - AVG(csat) OVER w_csat_channel, 2) AS csat_vs_csat_channel_avg
    FROM filtered_tickets
    WINDOW w_closed_at_month_by_closed_at AS (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY closed_at), w_channel_closed_at_to_date_by_3 AS (PARTITION BY channel, TO_DATE(closed_at) ORDER BY opened_at ASC), w_agent_id_by_opened_at AS (PARTITION BY agent_id ORDER BY opened_at, id), w_csat_channel AS (PARTITION BY csat, channel)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at,
        ROUND(opened_at_cume_dist * 1.5, 2) AS opened_at_cume_dist_adj,
        last_closed_at + INTERVAL 7 DAYS AS last_closed_at_plus_7d
    FROM with_trends
)
SELECT
    id,
    agent_id,
    priority,
    csat,
    tags,
    csat_channel_max_csat,
    requester_id_share_of_csat_channel,
    csat_change,
    rows_in_channel_agent_id,
    agent_id_closed_at_month_max_csat,
    csat_channel_min_csat,
    min_requester_id_so_far,
    opened_at_decile,
    opened_at_rank_2,
    last_closed_at,
    csat_vs_csat_channel_avg,
    opened_at_cume_dist_adj,
    last_closed_at_plus_7d,
    LENGTH(TRIM(priority)) AS priority_len,
    SIZE(tags) AS tags_count
FROM derived
WHERE opened_at_rank > 4
ORDER BY min_requester_id_so_far DESC, opened_at_cume_dist_adj, id
LIMIT 20
