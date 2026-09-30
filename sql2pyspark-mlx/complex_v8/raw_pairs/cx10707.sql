WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE (ARRAY_CONTAINS(tags, 'login') OR requester_id >= 4)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY agent_id, csat ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        MAX(csat) OVER (PARTITION BY channel) AS channel_max_csat,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_agent_id, 0), 4) AS requester_id_share_of_agent_id,
        COUNT(*) OVER w_agent_id AS rows_in_agent_id,
        FIRST_VALUE(requester_id) OVER (PARTITION BY agent_id, csat ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id,
        csat - LAG(csat) OVER (PARTITION BY agent_id, csat ORDER BY closed_at DESC, id) AS csat_change,
        SIZE(COLLECT_SET(channel) OVER w_agent_id) AS distinct_channel_in_agent_id,
        ROUND(csat - AVG(csat) OVER w_channel, 2) AS csat_vs_channel_avg,
        SUM(requester_id) OVER (PARTITION BY priority ORDER BY requester_id, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS requester_id_sum_2,
        ROW_NUMBER() OVER w_agent_id_csat_by_closed_at AS seq_no,
        ROUND(AVG(requester_id) OVER (PARTITION BY channel), 2) AS channel_avg_requester_id,
        MAX(csat) OVER (PARTITION BY agent_id) AS agent_id_max_csat,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(closed_at), channel ORDER BY requester_id ASC, id), 3) AS requester_id_cume_dist,
        LEAD(requester_id) OVER (PARTITION BY YEAR(closed_at), channel ORDER BY requester_id ASC, id) AS next_requester_id
    FROM cleaned
    WINDOW w_agent_id AS (PARTITION BY agent_id), w_channel AS (PARTITION BY channel), w_agent_id_csat_by_closed_at AS (PARTITION BY agent_id, csat ORDER BY closed_at DESC, id)
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(channel_avg_requester_id) OVER (PARTITION BY YEAR(closed_at)) AS closed_at_year_sum_channel_avg_requester_id,
        MIN(seq_no) OVER w_closed_at_year AS closed_at_year_min_seq_no,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY YEAR(closed_at))) AS distinct_channel_in_closed_at_year,
        LAST_VALUE(next_requester_id) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_next_requester_id,
        RANK() OVER (ORDER BY opened_at DESC) AS opened_at_rank,
        LAG(seq_no, 3, 0) OVER (ORDER BY opened_at DESC, id) AS prev3_seq_no,
        NTH_VALUE(csat_change, 2) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat_change
    FROM with_trends
    WINDOW w_closed_at_year AS (PARTITION BY YEAR(closed_at))
)
SELECT
    id,
    agent_id,
    priority,
    channel,
    csat,
    second_opened_at,
    channel_max_csat,
    requester_id_share_of_agent_id,
    rows_in_agent_id,
    first_requester_id,
    csat_vs_channel_avg,
    requester_id_sum_2,
    seq_no,
    channel_avg_requester_id,
    next_requester_id,
    closed_at_year_sum_channel_avg_requester_id,
    closed_at_year_min_seq_no,
    distinct_channel_in_closed_at_year,
    last_next_requester_id,
    opened_at_rank,
    DATE_TRUNC('DAY', closed_at) AS closed_at_day,
    opened_at + INTERVAL 15 MINUTES AS opened_at_plus_15m
FROM with_trends_2
