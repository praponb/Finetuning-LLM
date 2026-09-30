WITH base AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at
    FROM tickets
    WHERE channel IN ('web', 'phone')
      AND agent_id <= 6
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(hours_opened_at_to_closed_at) OVER (PARTITION BY agent_id) AS agent_id_min_hours_opened_at_to_closed_at,
        ROUND(csat - AVG(csat) OVER w_agent_id, 2) AS csat_vs_agent_id_avg,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY csat ORDER BY hours_opened_at_to_closed_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id,
        LEAD(csat, 3, 0) OVER w_agent_id_by_csat AS next3_csat,
        MAX(requester_id) OVER (PARTITION BY agent_id, csat) AS agent_id_csat_max_requester_id,
        SUM(requester_id) OVER (PARTITION BY csat ORDER BY hours_opened_at_to_closed_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        hours_opened_at_to_closed_at - LAG(hours_opened_at_to_closed_at) OVER w_agent_id_by_csat AS hours_opened_at_to_closed_at_change
    FROM base
    WINDOW w_agent_id AS (PARTITION BY agent_id), w_agent_id_by_csat AS (PARTITION BY agent_id ORDER BY csat, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(next3_csat, 0) AS next3_csat_filled,
        csat + requester_id AS csat_plus_requester_id
    FROM with_windows
    WHERE hours_opened_at_to_closed_at_change >= -1400.0
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    csat,
    tags,
    hours_opened_at_to_closed_at,
    agent_id_min_hours_opened_at_to_closed_at,
    csat_vs_agent_id_avg,
    second_requester_id,
    next3_csat,
    agent_id_csat_max_requester_id,
    hours_opened_at_to_closed_at_change,
    csat_plus_requester_id,
    ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
FROM derived
ORDER BY agent_id DESC
