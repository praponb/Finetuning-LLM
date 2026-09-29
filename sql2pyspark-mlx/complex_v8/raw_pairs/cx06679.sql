WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        csat,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix
    FROM tickets
    WHERE agent_id >= 9
),
joined AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.csat,
        s.channel_or_unknown,
        su.agent_name,
        su.team AS agent_team
    FROM recent_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        channel,
        csat,
        channel_or_unknown,
        agent_name,
        agent_team,
        ROUND(AVG(csat) OVER (PARTITION BY csat ORDER BY csat, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_3_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY csat DESC), 3) AS csat_percent_rank,
        MIN(csat) OVER (PARTITION BY channel, csat ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_so_far,
        MIN(csat) OVER (PARTITION BY csat, channel_or_unknown) AS csat_channel_or_unknown_min_csat,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_id), 2) AS csat_vs_agent_id_avg,
        ROUND(csat - AVG(csat) OVER (PARTITION BY csat, channel_or_unknown), 2) AS csat_vs_csat_channel_or_unknown_avg,
        ROUND(csat / NULLIF(SUM(csat) OVER w_csat_channel_or_unknown, 0), 4) AS csat_share_of_csat_channel_or_unknown,
        MIN(csat) OVER (PARTITION BY agent_id) AS agent_id_min_csat,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id,
        RANK() OVER (ORDER BY csat DESC) AS csat_rank,
        NTILE(3) OVER (ORDER BY csat DESC, id) AS csat_tercile
    FROM joined
    WINDOW w_csat_channel_or_unknown AS (PARTITION BY csat, channel_or_unknown)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE csat_rank = 1
      AND channel_or_unknown = 'web'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        channel,
        channel_or_unknown,
        agent_name,
        agent_team,
        csat_avg_3_prior,
        min_csat_so_far,
        csat_channel_or_unknown_min_csat,
        csat_vs_csat_channel_or_unknown_avg,
        rows_in_agent_id,
        csat_rank,
        csat_tercile,
        MAX(min_csat_so_far) OVER (PARTITION BY agent_id ORDER BY agent_id_min_csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_min_csat_so_far_so_far,
        ROUND(csat_percent_rank / NULLIF(SUM(csat_percent_rank) OVER w_channel, 0), 4) AS csat_percent_rank_share_of_channel,
        ROUND(csat_tercile / NULLIF(SUM(csat_tercile) OVER (PARTITION BY channel), 0), 4) AS csat_tercile_share_of_channel,
        NTH_VALUE(csat, 2) OVER (PARTITION BY agent_id ORDER BY agent_id_min_csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        LAG(csat_rank) OVER (PARTITION BY agent_id ORDER BY agent_id_min_csat DESC NULLS FIRST, id) AS prev_csat_rank,
        COUNT(*) OVER (PARTITION BY agent_team ORDER BY csat_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(csat_vs_agent_id_avg - AVG(csat_vs_agent_id_avg) OVER (PARTITION BY channel), 2) AS csat_vs_agent_id_avg_vs_channel_avg,
        MAX(agent_id_min_csat) OVER (PARTITION BY csat, channel) AS csat_channel_max_agent_id_min_csat
    FROM first_rows
    WINDOW w_channel AS (PARTITION BY channel)
)
SELECT
    id,
    agent_id,
    channel,
    channel_or_unknown,
    agent_name,
    csat_avg_3_prior,
    csat_channel_or_unknown_min_csat,
    rows_in_agent_id,
    csat_rank,
    csat_tercile,
    max_min_csat_so_far_so_far,
    csat_percent_rank_share_of_channel,
    csat_tercile_share_of_channel,
    second_csat,
    running_count,
    csat_vs_agent_id_avg_vs_channel_avg,
    csat_channel_max_agent_id_min_csat,
    ABS(csat_rank - 6) AS csat_rank_distance,
    COALESCE(channel, 'UNKNOWN') AS channel_or_unknown_2
FROM with_windows
ORDER BY max_min_csat_so_far_so_far DESC, channel DESC, id
LIMIT 20
