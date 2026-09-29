WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags
    FROM tickets
    WHERE SIZE(tags) >= 2
      AND requester_id > 8
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        LAST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY MONTH(opened_at) ORDER BY closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_requester_id,
        LEAD(requester_id, 3, 0) OVER (PARTITION BY agent_id ORDER BY requester_id ASC NULLS LAST, id) AS next3_requester_id,
        RANK() OVER (ORDER BY closed_at DESC, id) AS closed_at_rank,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY channel, YEAR(opened_at)), 0), 4) AS requester_id_share_of_channel_opened_at_year,
        MAX(requester_id) OVER (PARTITION BY channel, agent_id) AS channel_agent_id_max_requester_id,
        requester_id - LAG(requester_id) OVER (PARTITION BY agent_id ORDER BY requester_id ASC NULLS LAST, id) AS requester_id_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id ORDER BY requester_id ASC NULLS LAST, id), 3) AS requester_id_percent_rank,
        ROUND(AVG(requester_id) OVER (PARTITION BY channel, agent_id), 2) AS channel_agent_id_avg_requester_id,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(opened_at) ORDER BY closed_at ASC), 3) AS closed_at_cume_dist,
        RANK() OVER (PARTITION BY agent_id ORDER BY opened_at NULLS LAST, id) AS opened_at_rank,
        ROW_NUMBER() OVER w_all_by_closed_at AS seq_no
    FROM prepared
    WINDOW w_channel_agent_id AS (PARTITION BY channel, agent_id), w_all_by_closed_at AS (ORDER BY closed_at DESC, id)
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        closed_at_rank,
        channel_agent_id_max_requester_id,
        requester_id_change,
        channel_agent_id_avg_requester_id,
        closed_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY YEAR(closed_at), seq_no ORDER BY requester_id DESC, id) AS rn,
        RANK() OVER (PARTITION BY YEAR(closed_at), seq_no ORDER BY requester_id DESC, id) AS requester_id_rank,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY seq_no DESC, id) AS seq_no_seq,
        LAST_VALUE(requester_id_change) IGNORE NULLS OVER (PARTITION BY closed_at_rank ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_requester_id_change
    FROM metrics
)
SELECT
    id,
    opened_at,
    closed_at,
    closed_at_rank,
    channel_agent_id_max_requester_id,
    channel_agent_id_avg_requester_id,
    closed_at_cume_dist,
    rn,
    requester_id_rank,
    seq_no_seq,
    last_known_requester_id_change
FROM metrics_2
ORDER BY seq_no_seq DESC NULLS FIRST, closed_at
