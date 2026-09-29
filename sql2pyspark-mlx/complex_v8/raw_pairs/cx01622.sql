WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        DATE_TRUNC('WEEK', opened_at) AS opened_at_week,
        LENGTH(TRIM(priority)) AS priority_len,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at
    FROM tickets
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_channel_by_closed_at AS seq_no,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY channel), 0), 4) AS csat_share_of_channel,
        SIZE(COLLECT_SET(agent_id) OVER w_channel) AS distinct_agent_id_in_channel,
        ROUND(CUME_DIST() OVER w_all_by_priority_len_ties, 3) AS priority_len_cume_dist,
        SUM(csat) OVER (PARTITION BY channel) AS channel_sum_csat,
        ROUND(days_since_opened_at / NULLIF(SUM(days_since_opened_at) OVER (PARTITION BY channel), 0), 4) AS days_since_opened_at_share_of_channel
    FROM filtered_tickets
    WINDOW w_channel_by_closed_at AS (PARTITION BY channel ORDER BY closed_at DESC, id), w_channel AS (PARTITION BY channel), w_all_by_priority_len_ties AS (ORDER BY priority_len)
),
per_label AS (
    -- One row per tag
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.csat,
        s.opened_at_week,
        s.priority_len,
        s.days_since_opened_at,
        s.seq_no,
        s.csat_share_of_channel,
        s.distinct_agent_id_in_channel,
        s.priority_len_cume_dist,
        s.channel_sum_csat,
        s.days_since_opened_at_share_of_channel,
        t.tag
    FROM windowed s
    LATERAL VIEW OUTER EXPLODE(s.tags) t AS tag
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('DAY', closed_at) AS closed_at_day,
        QUARTER(opened_at_week) AS opened_at_week_quarter
    FROM per_label
    WHERE distinct_agent_id_in_channel > 5
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT channel, tag, days_since_opened_at
    FROM derived
) src
PIVOT (
    AVG(days_since_opened_at)
    FOR tag IN ('bug', 'upgrade', 'login')
)
