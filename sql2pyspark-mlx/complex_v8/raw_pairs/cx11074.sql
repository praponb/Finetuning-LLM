WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        csat + requester_id AS csat_plus_requester_id
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_group AS (
    -- Summarise per group
    SELECT
        TO_DATE(opened_at) AS opened_at_day,
        SUM(CASE WHEN priority <> 'P4' THEN starts_session ELSE 0 END) AS starts_session_priority_p4,
        COUNT(DISTINCT priority) AS distinct_prioritys,
        MAX(session_no) AS max_session_no,
        COUNT(DISTINCT channel) AS distinct_channels
    FROM sessionized
    GROUP BY TO_DATE(opened_at)
),
enriched AS (
    -- Derive helper columns
    SELECT
        opened_at_day,
        starts_session_priority_p4,
        distinct_prioritys,
        max_session_no,
        CASE WHEN distinct_prioritys >= 9 THEN 'large' WHEN distinct_prioritys >= 5 THEN 'medium' ELSE 'small' END AS distinct_prioritys_tier
    FROM per_group
    WHERE distinct_prioritys < 8
)
-- Subtotals with ROLLUP
SELECT
    distinct_prioritys,
    starts_session_priority_p4,
    GROUPING_ID() AS grouping_level,
    MIN(opened_at_day) AS first_opened_at_day,
    DATEDIFF(MAX(opened_at_day), MIN(opened_at_day)) AS opened_at_day_span_days,
    PERCENTILE_APPROX(distinct_prioritys, 0.5) AS median_distinct_prioritys
FROM enriched
GROUP BY ROLLUP(distinct_prioritys, starts_session_priority_p4)
