WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags
    FROM tickets
    WHERE channel IN ('web', 'phone', 'chat')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_at,
        closed_at,
        channel,
        tags,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY priority, agent_id), 2) AS requester_id_vs_priority_agent_id_avg,
        FIRST_VALUE(opened_at) OVER (ORDER BY closed_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at
    FROM scoped
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY TO_DATE(closed_at), channel ORDER BY first_opened_at, id) AS first_opened_at_quartile,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(closed_at), channel ORDER BY first_opened_at, id) AS first_opened_at_seq,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(closed_at), channel ORDER BY first_opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at
    FROM metrics
    WINDOW w_closed_at_to_date_channel_by AS (PARTITION BY TO_DATE(closed_at), channel ORDER BY first_opened_at, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        ABS(first_opened_at_quartile - 6) AS first_opened_at_quartile_distance,
        HOUR(closed_at) AS closed_at_hour
    FROM metrics_2
)
SELECT DISTINCT
    first_opened_at_quartile,
    first_opened_at_seq,
    first_opened_at_quartile_distance
FROM enriched
WHERE MONTH(closed_at) IN (1, 3)
ORDER BY first_opened_at_quartile, first_opened_at_seq DESC NULLS FIRST
