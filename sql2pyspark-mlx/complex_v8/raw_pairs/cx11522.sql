WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE SIZE(tags) >= 2
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel_len,
        ROUND(CUME_DIST() OVER (PARTITION BY channel ORDER BY closed_at), 3) AS closed_at_cume_dist,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_channel, 0), 4) AS requester_id_share_of_channel,
        COUNT(*) OVER w_channel AS rows_in_channel,
        SUM(requester_id) OVER (PARTITION BY channel_len ORDER BY channel_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        channel_len - LAG(channel_len) OVER w_channel_len_by_channel_len AS channel_len_change,
        NTILE(4) OVER (ORDER BY requester_id DESC, id) AS requester_id_quartile,
        SUM(requester_id) OVER (ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id_2,
        ROUND(channel_len / NULLIF(SUM(channel_len) OVER (PARTITION BY channel), 0), 4) AS channel_len_share_of_channel,
        NTILE(4) OVER (PARTITION BY channel_len ORDER BY channel_len, id) AS channel_len_quartile,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY channel), 2) AS requester_id_vs_channel_avg,
        SIZE(COLLECT_SET(agent_id) OVER w_channel) AS distinct_agent_id_in_channel,
        FIRST_VALUE(channel_len) IGNORE NULLS OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel_len
    FROM filtered
    WINDOW w_channel AS (PARTITION BY channel), w_channel_len_by_channel_len AS (PARTITION BY channel_len ORDER BY channel_len, id)
)
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    channel_len,
    closed_at_cume_dist,
    requester_id_share_of_channel,
    rows_in_channel,
    running_requester_id,
    channel_len_change,
    requester_id_quartile,
    channel_len_quartile,
    requester_id_vs_channel_avg,
    distinct_agent_id_in_channel,
    first_known_channel_len,
    TO_DATE(closed_at) AS closed_at_date
FROM metrics
ORDER BY id DESC NULLS FIRST, channel_len_quartile
