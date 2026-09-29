WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix,
        COALESCE(requester_id, 0) AS requester_id_filled
    FROM tickets
    WHERE requester_id >= 4
      AND priority IN ('P2', 'P1', 'P4')
),
unique_rows AS (
    -- Keep the earliest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        priority_prefix,
        requester_id_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY requester_id_filled, id) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
channel_stats AS (
    -- Max csat per channel
    SELECT channel, MAX(csat) AS max_csat
    FROM unique_rows
    GROUP BY channel
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_csat, ROUND(s.csat - g.max_csat, 2) AS csat_minus_max
    FROM unique_rows s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.csat = g.max_csat
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_id, csat, channel, csat_minus_max
    FROM compared
) src
PIVOT (
    COUNT(csat_minus_max) AS count_v, MAX(csat_minus_max) AS max_v
    FOR channel IN ('chat', 'phone', 'web', 'email')
)
