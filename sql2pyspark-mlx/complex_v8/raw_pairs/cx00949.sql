WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM tickets
    WHERE csat BETWEEN 2 AND 5
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(requester_id_per_csat) IGNORE NULLS OVER (PARTITION BY channel, priority ORDER BY requester_id_per_csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_requester_id_per_csat,
        LEAD(requester_id_per_csat) OVER (PARTITION BY priority ORDER BY requester_id_per_csat DESC NULLS FIRST, id) AS next_requester_id_per_csat,
        ROW_NUMBER() OVER (PARTITION BY priority ORDER BY opened_at, id) AS opened_at_seq,
        ROUND(requester_id_per_csat / NULLIF(SUM(requester_id_per_csat) OVER (PARTITION BY channel), 0), 4) AS requester_id_per_csat_share_of_channel,
        requester_id_per_csat - LAG(requester_id_per_csat) OVER (PARTITION BY channel, priority ORDER BY requester_id_per_csat DESC NULLS FIRST, id) AS requester_id_per_csat_change,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel,
        NTH_VALUE(requester_id_per_csat, 2) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id_per_csat,
        DENSE_RANK() OVER (PARTITION BY priority ORDER BY opened_at, id) AS opened_at_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority ORDER BY requester_id_per_csat DESC NULLS FIRST, id), 3) AS requester_id_per_csat_percent_rank,
        ROUND(requester_id_per_csat - AVG(requester_id_per_csat) OVER (PARTITION BY channel), 2) AS requester_id_per_csat_vs_channel_avg,
        MAX(requester_id_per_csat) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS requester_id_per_csat_max_7,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel)) AS distinct_agent_id_in_channel,
        SUM(requester_id_per_csat) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id_per_csat,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY priority ORDER BY requester_id_per_csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        SIZE(COLLECT_SET(priority) OVER w_channel) AS distinct_priority_in_channel
    FROM cleaned
    WINDOW w_channel AS (PARTITION BY channel)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE opened_at_dense_rank = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        channel,
        requester_id_per_csat,
        last_known_requester_id_per_csat,
        next_requester_id_per_csat,
        opened_at_seq,
        requester_id_per_csat_share_of_channel,
        requester_id_per_csat_change,
        rows_in_channel,
        second_requester_id_per_csat,
        requester_id_per_csat_percent_rank,
        requester_id_per_csat_vs_channel_avg,
        running_requester_id_per_csat,
        last_known_opened_at,
        distinct_priority_in_channel,
        ROUND(rows_in_channel - AVG(rows_in_channel) OVER (PARTITION BY distinct_priority_in_channel, opened_at_seq), 2) AS rows_in_channel_vs_distinct_priority_in_channel_,
        ROW_NUMBER() OVER (PARTITION BY distinct_priority_in_channel, channel ORDER BY requester_id_per_csat_share_of_channel, id) AS seq_no,
        RANK() OVER (PARTITION BY distinct_priority_in_channel, channel ORDER BY requester_id_per_csat_share_of_channel) AS requester_id_per_csat_share_of_channel_rank,
        FIRST_VALUE(opened_at_seq) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at_seq,
        next_requester_id_per_csat - LAG(next_requester_id_per_csat) OVER (ORDER BY opened_at DESC, id) AS next_requester_id_per_csat_change,
        SUM(requester_id_per_csat_max_7) OVER (PARTITION BY distinct_priority_in_channel, opened_at_seq) AS distinct_priority_in_channel_opened_at_seq_sum_r,
        SUM(second_requester_id_per_csat) OVER (PARTITION BY distinct_priority_in_channel, opened_at_seq) AS distinct_priority_in_channel_opened_at_seq_sum_s
    FROM first_rows
),
grouped AS (
    -- Group-level totals
    SELECT
        agent_id,
        TO_DATE(last_known_opened_at) AS last_known_opened_at_day,
        MAX(last_known_opened_at) AS last_last_known_opened_at,
        MIN(running_requester_id_per_csat) AS min_running_requester_id_per_csat,
        COUNT(DISTINCT channel) AS distinct_channels,
        SUM(CASE WHEN channel NOT IN ('email', 'phone') THEN requester_id_per_csat_share_of_channel_rank ELSE 0 END) AS requester_id_per_csat_share_of_channel_rank_chan,
        COUNT(*) AS cnt,
        MIN(first_opened_at_seq) AS min_first_opened_at_seq
    FROM sequenced
    GROUP BY agent_id, TO_DATE(last_known_opened_at)
)
SELECT
    agent_id,
    last_known_opened_at_day,
    last_last_known_opened_at,
    min_running_requester_id_per_csat,
    distinct_channels,
    requester_id_per_csat_share_of_channel_rank_chan,
    cnt,
    min_first_opened_at_seq,
    CASE WHEN min_first_opened_at_seq >= 9 THEN 'large' WHEN min_first_opened_at_seq >= 5 THEN 'medium' ELSE 'small' END AS min_first_opened_at_seq_tier
FROM grouped
