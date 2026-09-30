WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        TO_DATE(opened_at) AS opened_at_date
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-03-09 04:00:00'
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        requester_id,
        opened_at,
        channel,
        opened_at_date,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at
    FROM valid_tickets
    WHERE agent_id <= 8
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(opened_at_date) AS opened_at_date_year,
        channel,
        DATEDIFF(MAX(opened_at_date), MIN(opened_at_date)) AS opened_at_date_span_days,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(days_since_opened_at) AS total_days_since_opened_at,
        COUNT(*) AS row_count,
        SUM(CASE WHEN channel IS NOT NULL THEN requester_id ELSE 0 END) AS requester_id_channel_is_not_null
    FROM annotated
    GROUP BY YEAR(opened_at_date), channel
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT row_count, channel, opened_at_date_span_days
    FROM rollup_base
) src
PIVOT (
    SUM(opened_at_date_span_days)
    FOR channel IN ('web', 'phone')
)
