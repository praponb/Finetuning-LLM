WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        TO_DATE(closed_at) AS closed_at_date
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND closed_at >= TIMESTAMP '2025-01-17 21:00:00'
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        TO_DATE(opened_at) AS opened_at_date,
        CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref
    FROM scoped_tickets
    WHERE channel = 'web'
),
stats AS (
    -- Group-level totals
    SELECT
        channel,
        COUNT(*) FILTER (WHERE channel NOT IN ('email', 'phone')) AS n_channel_not_in_email_phone,
        MIN(opened_at_date) AS first_opened_at_date
    FROM annotated
    WHERE TO_DATE(opened_at) < DATE '2025-02-06'
    GROUP BY channel
),
tagged AS (
    -- Add calculated columns
    SELECT
        channel,
        n_channel_not_in_email_phone,
        first_opened_at_date,
        LENGTH(TRIM(channel)) AS channel_len,
        CASE WHEN channel IN ('web', 'chat') THEN 'group_a' WHEN channel = 'email' THEN 'group_b' ELSE 'other' END AS channel_group,
        CASE WHEN channel IN ('chat', 'web') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group_2
    FROM stats
),
channel_group_stats AS (
    -- Max n_channel_not_in_email_phone per channel_group
    SELECT channel_group, MAX(n_channel_not_in_email_phone) AS max_n_channel_not_in_email_phone
    FROM tagged
    GROUP BY channel_group
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_n_channel_not_in_email_phone,
        ROUND(s.n_channel_not_in_email_phone - g.max_n_channel_not_in_email_phone, 2) AS n_channel_not_in_email_phone_minus_max
    FROM tagged s
    JOIN channel_group_stats g ON g.channel_group = s.channel_group
    WHERE s.n_channel_not_in_email_phone = g.max_n_channel_not_in_email_phone
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(channel_group, '#', CAST(channel_len AS STRING)) AS channel_group_ref
    FROM vs_group
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(n_channel_not_in_email_phone) OVER (ORDER BY channel_len DESC, channel ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS n_channel_not_in_email_phone_sum_7,
        SIZE(COLLECT_SET(channel_group_2) OVER (PARTITION BY channel_group, channel)) AS distinct_channel_group_2_in_channel_group_channe,
        RANK() OVER w_all_by_channel_len_ties AS channel_len_rank
    FROM enriched
    WINDOW w_all_by_channel_len_ties AS (ORDER BY channel_len DESC)
)
-- Final projection
SELECT
    channel,
    n_channel_not_in_email_phone,
    first_opened_at_date,
    channel_len,
    channel_group_2,
    max_n_channel_not_in_email_phone,
    n_channel_not_in_email_phone_minus_max,
    channel_group_ref,
    n_channel_not_in_email_phone_sum_7
FROM with_windows
ORDER BY channel_len DESC, channel
LIMIT 5
