WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        CONCAT(channel, '#', CAST(csat AS STRING)) AS channel_ref,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at,
        DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label
    FROM tickets
    WHERE csat < 4
),
grouped AS (
    -- Summarise per group
    SELECT
        TO_DATE(closed_at) AS closed_at_day,
        SUM(CASE WHEN channel = 'phone' THEN requester_id ELSE 0 END) AS requester_id_channel_phone,
        MAX(closed_at) AS last_closed_at,
        COUNT(DISTINCT channel) AS distinct_channels
    FROM recent
    GROUP BY 1
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE requester_id_channel_phone > (SELECT AVG(requester_id_channel_phone) FROM grouped)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(requester_id_channel_phone) OVER w_last_closed_at_year_distinct, 2) AS last_closed_at_year_distinct_channels_avg_reques,
        LEAD(closed_at_day, 2) OVER (ORDER BY requester_id_channel_phone DESC, closed_at_day) AS next2_closed_at_day
    FROM with_activity
    WINDOW w_last_closed_at_year_distinct AS (PARTITION BY YEAR(last_closed_at), distinct_channels)
),
scored AS (
    -- Label and score rows
    SELECT
        closed_at_day,
        requester_id_channel_phone,
        distinct_channels,
        DATE_FORMAT(closed_at_day, 'yyyy-MM-dd') AS closed_at_day_label
    FROM ranked
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored
    WHERE requester_id_channel_phone > (SELECT AVG(requester_id_channel_phone) FROM scored)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(distinct_channels) OVER (PARTITION BY distinct_channels) AS distinct_channels_min_distinct_channels,
        LAST_VALUE(requester_id_channel_phone) OVER (PARTITION BY distinct_channels ORDER BY distinct_channels ASC, closed_at_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id_channel_phone,
        MAX(distinct_channels) OVER (PARTITION BY distinct_channels) AS distinct_channels_max_distinct_channels,
        LAG(distinct_channels, 2, 0) OVER (ORDER BY distinct_channels DESC, closed_at_day) AS prev2_distinct_channels,
        COUNT(*) OVER (PARTITION BY distinct_channels) AS rows_in_distinct_channels,
        LAG(closed_at_day, 3) OVER (ORDER BY distinct_channels DESC, closed_at_day) AS prev3_closed_at_day
    FROM screened
)
-- Subtotals with GROUPING SETS
SELECT
    prev2_distinct_channels,
    distinct_channels,
    MAX(closed_at_day) AS last_closed_at_day
FROM with_windows
GROUP BY GROUPING SETS ((prev2_distinct_channels, distinct_channels), (prev2_distinct_channels))
