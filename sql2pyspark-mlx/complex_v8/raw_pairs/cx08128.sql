WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        priority,
        channel,
        tags,
        COALESCE(requester_id, 0) AS requester_id_filled,
        CONCAT(priority, '#', CAST(csat AS STRING)) AS priority_ref,
        DATE_TRUNC('WEEK', closed_at) AS closed_at_week
    FROM tickets
    WHERE channel = 'email'
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM valid s
),
ticket_stats_2 AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MAX(closed_at) AS last_ticket_closed_at,
        SUM(csat) AS total_ticket_csat,
        MIN(opened_at) AS first_ticket_opened_at
    FROM tickets
    GROUP BY agent_id
),
summarized_rows_2 AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.last_ticket_closed_at,
        t.total_ticket_csat,
        t.first_ticket_opened_at
    FROM summarized_rows s
    LEFT JOIN ticket_stats_2 t ON t.agent_id = s.agent_id
),
exploded AS (
    -- One row per tag
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.channel,
        s.requester_id_filled,
        s.priority_ref,
        s.closed_at_week,
        s.ticket_count,
        s.last_ticket_closed_at,
        s.total_ticket_csat,
        s.first_ticket_opened_at,
        t.tag
    FROM summarized_rows_2 s
    LATERAL VIEW OUTER EXPLODE(s.tags) t AS tag
),
rollup_base AS (
    -- Summarise per group
    SELECT
        tag,
        MAX(closed_at_week) AS last_closed_at_week,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(requester_id_filled, 0.5) AS median_requester_id_filled,
        SUM(ticket_count) AS total_ticket_count
    FROM exploded
    GROUP BY 1
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(row_count / 2) * 2 AS row_count_bucket,
        LOWER(REGEXP_REPLACE(TRIM(tag), ' ', '_')) AS tag_slug
    FROM rollup_base
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY total_ticket_count, tag) AS seq_no,
        SUM(total_ticket_count) OVER (ORDER BY total_ticket_count, tag ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS total_ticket_count_sum_3_prior
    FROM scored
)
-- Final projection
SELECT
    tag,
    last_closed_at_week,
    row_count,
    median_requester_id_filled,
    total_ticket_count,
    row_count_bucket,
    tag_slug,
    seq_no,
    total_ticket_count_sum_3_prior,
    DATE_TRUNC('HOUR', last_closed_at_week) AS last_closed_at_week_hour,
    DATE_TRUNC('HOUR', last_closed_at_week) AS last_closed_at_week_hour_2
FROM windowed
WHERE total_ticket_count_sum_3_prior BETWEEN 29 AND 94
ORDER BY tag_slug DESC NULLS FIRST, row_count
