WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND csat < 4
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority_or_unknown ORDER BY opened_at, id), 3) AS opened_at_percent_rank,
        ROUND(greatest_csat_requester_id / NULLIF(SUM(greatest_csat_requester_id) OVER (PARTITION BY priority_or_unknown, agent_id), 0), 4) AS greatest_csat_requester_id_share_of_priority_or_,
        NTH_VALUE(greatest_csat_requester_id, 2) OVER (PARTITION BY agent_id, channel ORDER BY greatest_csat_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_greatest_csat_requester_id,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY priority_or_unknown, agent_id)) AS distinct_channel_in_priority_or_unknown_agent_id
    FROM filtered
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        greatest_csat_requester_id,
        priority_or_unknown,
        opened_at_percent_rank,
        greatest_csat_requester_id_share_of_priority_or_,
        second_greatest_csat_requester_id,
        LOWER(REGEXP_REPLACE(TRIM(priority_or_unknown), ' ', '_')) AS priority_or_unknown_slug
    FROM with_trends
    WHERE opened_at_percent_rank >= 0.2
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, metric_value
FROM (
    SELECT id, greatest_csat_requester_id, second_greatest_csat_requester_id
    FROM derived
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (greatest_csat_requester_id AS greatest_csat_requester_id_v, second_greatest_csat_requester_id AS second_greatest_csat_requester_id_v)
) unp
