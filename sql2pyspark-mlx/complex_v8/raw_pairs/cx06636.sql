WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        DATE_TRUNC('QUARTER', closed_at) AS closed_at_quarter
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-16 00:00:00'
      AND csat BETWEEN 2 AND 4
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        opened_at,
        priority,
        channel,
        tags,
        SUM(requester_id) OVER (ORDER BY closed_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        MAX(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at)) AS opened_at_month_max_requester_id,
        FIRST_VALUE(requester_id) OVER (ORDER BY closed_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id
    FROM cleaned_tickets
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        opened_at,
        channel,
        tags,
        first_requester_id,
        FLOOR(first_requester_id / 5) * 5 AS first_requester_id_bucket
    FROM with_windows
    WHERE opened_at >= TIMESTAMP '2025-01-19 07:00:00'
)
SELECT
    id,
    requester_id,
    opened_at,
    channel,
    tags,
    first_requester_id,
    first_requester_id_bucket,
    DATE_FORMAT(opened_at, 'yyyy-MM-dd HH:00') AS opened_at_label
FROM derived
