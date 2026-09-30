WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT(channel, '#', CAST(requester_id AS STRING)) AS channel_ref,
        CASE WHEN channel IN ('phone', 'web') THEN 'group_a' WHEN channel = 'email' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE (ARRAY_CONTAINS(tags, 'refund') OR csat >= 2)
),
enriched AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.channel_group,
        su.agent_name,
        su.team AS agent_team
    FROM cleaned s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        csat,
        tags,
        channel_group,
        agent_name,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY priority, csat ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        MAX(csat) OVER (PARTITION BY channel_group, priority) AS channel_group_priority_max_csat,
        ROUND(AVG(csat) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_2_prior,
        COUNT(*) OVER w_channel_group_priority AS rows_in_channel_group_priority,
        SIZE(COLLECT_SET(channel) OVER w_channel_group_priority) AS distinct_channel_in_channel_group_priority,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY csat ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at_2,
        ROUND(AVG(csat) OVER w_channel_group_priority, 2) AS channel_group_priority_avg_csat,
        SUM(csat) OVER w_channel_group_priority AS channel_group_priority_sum_csat,
        LEAD(csat, 3) OVER w_all_by_opened_at AS next3_csat
    FROM enriched
    WINDOW w_channel_group_priority AS (PARTITION BY channel_group, priority), w_all_by_opened_at AS (ORDER BY opened_at DESC, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        tags,
        channel_group,
        agent_name,
        second_opened_at,
        channel_group_priority_max_csat,
        csat_avg_2_prior,
        rows_in_channel_group_priority,
        channel_group_priority_sum_csat,
        next3_csat,
        DATEDIFF(DATE '2025-12-31', second_opened_at) AS days_since_second_opened_at,
        ARRAY_CONTAINS(tags, 'outage') AS has_outage,
        DATE_TRUNC('YEAR', opened_at) AS opened_at_year
    FROM ranked
    WHERE opened_at < TIMESTAMP '2025-02-24 09:00:00'
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(csat_avg_2_prior) OVER (PARTITION BY next3_csat) AS next3_csat_min_csat_avg_2_prior,
        MAX(csat_avg_2_prior) OVER (ORDER BY csat_avg_2_prior NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS csat_avg_2_prior_max_2_prior,
        NTH_VALUE(second_opened_at, 2) OVER (ORDER BY csat_avg_2_prior NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_opened_at,
        LEAD(days_since_second_opened_at, 2) OVER w_all_by_csat_avg_2_prior AS next2_days_since_second_opened_at,
        SUM(csat_avg_2_prior) OVER (PARTITION BY next3_csat) AS next3_csat_sum_csat_avg_2_prior
    FROM annotated
    WINDOW w_all_by_csat_avg_2_prior AS (ORDER BY csat_avg_2_prior NULLS LAST, id)
)
SELECT
    id,
    agent_id,
    channel,
    channel_group,
    agent_name,
    second_opened_at,
    channel_group_priority_max_csat,
    csat_avg_2_prior,
    rows_in_channel_group_priority,
    channel_group_priority_sum_csat,
    days_since_second_opened_at,
    has_outage,
    opened_at_year,
    next3_csat_min_csat_avg_2_prior,
    csat_avg_2_prior_max_2_prior,
    second_second_opened_at,
    next2_days_since_second_opened_at,
    next3_csat_sum_csat_avg_2_prior,
    LENGTH(TRIM(agent_name)) AS agent_name_len,
    COALESCE(next3_csat, 0) AS next3_csat_filled
FROM ranked_2
ORDER BY rows_in_channel_group_priority DESC, id
LIMIT 20
