WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        CASE WHEN channel IN ('email', 'web') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE csat > 2
      AND TO_DATE(opened_at) < DATE '2025-03-02'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(requester_id) OVER w_agent_id AS agent_id_min_requester_id,
        MAX(requester_id) OVER (PARTITION BY agent_id) AS agent_id_max_requester_id,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel,
        SUM(requester_id) OVER (PARTITION BY channel ORDER BY requester_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS requester_id_sum_4,
        ROUND(AVG(requester_id) OVER (PARTITION BY channel ORDER BY closed_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_3_prior,
        LAG(requester_id, 2, 0) OVER (PARTITION BY channel ORDER BY requester_id, id) AS prev2_requester_id,
        SUM(requester_id) OVER (PARTITION BY agent_id ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        LAST_VALUE(closed_at) OVER (ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at
    FROM filtered
    WINDOW w_agent_id AS (PARTITION BY agent_id)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        requester_id_avg_3_prior - requester_id_sum_4 AS requester_id_avg_3_prior_minus_requester_id_sum_,
        YEAR(last_closed_at) AS last_closed_at_year
    FROM with_windows
    WHERE running_requester_id <= 163
)
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    channel,
    channel_group,
    rows_in_channel,
    requester_id_sum_4,
    requester_id_avg_3_prior,
    last_closed_at,
    ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_opened_at,
    LOWER(REGEXP_REPLACE(TRIM(channel_group), ' ', '_')) AS channel_group_slug
FROM enriched
ORDER BY channel_group_slug
