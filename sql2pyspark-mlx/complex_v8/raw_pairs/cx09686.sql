WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        DATE_TRUNC('HOUR', closed_at) AS closed_at_hour,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE csat < 4
      AND priority = 'P2'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(days_since_opened_at) OVER (PARTITION BY channel, TO_DATE(closed_at)) AS channel_closed_at_to_date_min_days_since_opened_,
        LAG(closed_at, 2) OVER (PARTITION BY agent_id, YEAR(closed_at) ORDER BY days_since_opened_at, id) AS prev2_closed_at,
        MAX(days_since_opened_at) OVER (PARTITION BY channel, TO_DATE(closed_at)) AS channel_closed_at_to_date_max_days_since_opened_,
        LAST_VALUE(closed_at_hour) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at_hour,
        ROUND(days_since_opened_at / NULLIF(SUM(days_since_opened_at) OVER (PARTITION BY channel, TO_DATE(closed_at)), 0), 4) AS days_since_opened_at_share_of_channel_closed_at_,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel, TO_DATE(closed_at))) AS distinct_agent_id_in_channel_closed_at_to_date,
        SUM(days_since_opened_at) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS days_since_opened_at_sum_1_prior,
        ROUND(days_since_opened_at - AVG(days_since_opened_at) OVER w_channel_closed_at_to_date, 2) AS days_since_opened_at_vs_channel_closed_at_to_dat
    FROM base
    WINDOW w_channel_closed_at_to_date AS (PARTITION BY channel, TO_DATE(closed_at))
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    priority,
    channel,
    days_since_opened_at,
    channel_closed_at_to_date_max_days_since_opened_,
    last_closed_at_hour,
    days_since_opened_at_sum_1_prior,
    days_since_opened_at_vs_channel_closed_at_to_dat,
    DAYOFWEEK(closed_at) AS closed_at_dayofweek,
    COALESCE(days_since_opened_at, 0) AS days_since_opened_at_filled
FROM sequenced
ORDER BY agent_id DESC, days_since_opened_at_vs_channel_closed_at_to_dat, id
LIMIT 20
