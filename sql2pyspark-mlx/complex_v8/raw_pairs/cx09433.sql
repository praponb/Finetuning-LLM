WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix,
        SIZE(tags) AS tags_count
    FROM tickets
    WHERE csat <= 4
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(csat) OVER (PARTITION BY priority, agent_id ORDER BY requester_id DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS csat_sum_2_prior,
        ROUND(AVG(csat) OVER (PARTITION BY priority, tags_count), 2) AS priority_tags_count_avg_csat,
        NTILE(5) OVER w_tags_count_closed_at_to_date AS closed_at_quintile,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority, tags_count)) AS distinct_agent_id_in_priority_tags_count
    FROM prepared_tickets
    WINDOW w_tags_count_closed_at_to_date AS (PARTITION BY tags_count, TO_DATE(closed_at) ORDER BY closed_at DESC, id)
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', closed_at) AS closed_at_month,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        ROUND(AVG(tags_count), 2) AS avg_tags_count,
        ROUND(100 * COUNT(CASE WHEN channel IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_channel_is_not_null,
        SUM(CASE WHEN channel IN ('web', 'chat', 'phone') THEN 1 ELSE 0 END) AS n_channel_in_web_chat_phone
    FROM with_trends
    WHERE agent_id IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', closed_at)
    HAVING MAX(priority_tags_count_avg_csat) > 1.6
)
SELECT
    closed_at_month,
    channel_list,
    avg_tags_count,
    pct_channel_is_not_null,
    n_channel_in_web_chat_phone,
    DATEDIFF(DATE '2025-06-30', closed_at_month) AS days_since_closed_at_month,
    CASE WHEN avg_tags_count >= 2.1 THEN 'A' WHEN avg_tags_count >= 0.6 THEN 'B' ELSE 'C' END AS avg_tags_count_tier
FROM totals
