WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat
    FROM tickets
    WHERE MONTH(opened_at) IN (1, 2)
      AND channel IN ('email', 'web')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        closed_at,
        COUNT(*) OVER (PARTITION BY agent_id, csat ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat), 0), 4) AS csat_share_of_csat,
        COUNT(*) OVER (PARTITION BY agent_id, csat) AS rows_in_agent_id_csat,
        FIRST_VALUE(closed_at) OVER (PARTITION BY csat, channel ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        ROUND(AVG(csat) OVER (PARTITION BY YEAR(closed_at), agent_id ORDER BY csat NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS csat_avg_2,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY agent_id, csat)) AS distinct_channel_in_agent_id_csat,
        csat - LAG(csat) OVER (PARTITION BY YEAR(closed_at), agent_id ORDER BY csat NULLS LAST, id) AS csat_change,
        RANK() OVER (PARTITION BY agent_id ORDER BY csat) AS csat_rank
    FROM valid_tickets
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE csat_rank <= 5
      AND distinct_channel_in_agent_id_csat >= 3
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        csat_share_of_csat,
        first_closed_at,
        csat_avg_2,
        distinct_channel_in_agent_id_csat,
        csat_change,
        LAG(csat_change) OVER (ORDER BY distinct_channel_in_agent_id_csat DESC, id) AS prev_csat_change,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY csat_change, distinct_channel_in_agent_id_csat)) AS distinct_agent_id_in_csat_change_distinct_channe,
        SUM(csat_avg_2) OVER (PARTITION BY csat_change ORDER BY distinct_channel_in_agent_id_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat_avg_2,
        NTILE(3) OVER (ORDER BY csat_share_of_csat DESC, id) AS csat_share_of_csat_tercile,
        COUNT(*) OVER (PARTITION BY csat_change, distinct_channel_in_agent_id_csat) AS rows_in_csat_change_distinct_channel_in_agent_id,
        LAST_VALUE(csat_change) OVER (PARTITION BY distinct_channel_in_agent_id_csat, agent_id ORDER BY csat_avg_2 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat_change,
        ROUND(running_count / NULLIF(SUM(running_count) OVER (PARTITION BY csat_rank), 0), 4) AS running_count_share_of_csat_rank
    FROM top_rows
)
SELECT
    id,
    agent_id,
    csat_share_of_csat,
    csat_avg_2,
    distinct_channel_in_agent_id_csat,
    distinct_agent_id_in_csat_change_distinct_channe,
    rows_in_csat_change_distinct_channel_in_agent_id,
    last_csat_change
FROM metrics
ORDER BY last_csat_change, id DESC
