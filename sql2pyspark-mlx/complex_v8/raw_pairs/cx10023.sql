WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at
    FROM tickets
    WHERE priority = 'P1'
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        UPPER(TRIM(priority)) AS priority_clean,
        CONCAT_WS('-', priority, channel) AS priority_channel_label,
        DATE_FORMAT(closed_at, 'MMM yyyy') AS closed_at_label
    FROM valid
),
derived AS (
    -- Add calculated columns
    SELECT *, ABS(csat - 3) AS csat_distance
    FROM tagged
),
agent_id_stats AS (
    -- Avg days_since_closed_at per agent_id
    SELECT agent_id, AVG(days_since_closed_at) AS avg_days_since_closed_at
    FROM derived
    GROUP BY agent_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_closed_at,
        ROUND(s.days_since_closed_at - g.avg_days_since_closed_at, 2) AS days_since_closed_at_minus_avg
    FROM derived s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.days_since_closed_at > g.avg_days_since_closed_at
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id,
        ROW_NUMBER() OVER (ORDER BY days_since_closed_at, id) AS days_since_closed_at_seq,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY csat_distance, channel ORDER BY days_since_closed_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at
    FROM above_average
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    channel,
    tags,
    days_since_closed_at,
    priority_clean,
    priority_channel_label,
    closed_at_label,
    csat_distance,
    avg_days_since_closed_at,
    days_since_closed_at_minus_avg,
    rows_in_agent_id,
    days_since_closed_at_seq,
    second_closed_at,
    COALESCE(requester_id, 0) AS requester_id_filled
FROM windowed
ORDER BY days_since_closed_at_minus_avg
