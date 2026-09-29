WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label,
        CASE WHEN channel IN ('email', 'web') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND HOUR(closed_at) BETWEEN 9 AND 12
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(closed_at) AS closed_at_year,
        MIN(opened_at) AS first_opened_at,
        MIN(closed_at) AS first_closed_at
    FROM valid_tickets
    GROUP BY YEAR(closed_at)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(first_closed_at) - UNIX_TIMESTAMP(first_opened_at)) / 3600, 1) AS hours_first_opened_at_to_first_closed_at,
        DATEDIFF(DATE '2025-06-30', first_closed_at) AS days_since_first_closed_at
    FROM summary
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(closed_at_year, 0) AS closed_at_year_filled,
        ROUND((UNIX_TIMESTAMP(first_closed_at) - UNIX_TIMESTAMP(first_opened_at)) / 3600, 1) AS hours_first_opened_at_to_first_closed_at_2
    FROM derived
    WHERE first_closed_at < TIMESTAMP '2025-02-10 04:00:00'
)
-- Subtotals with CUBE
SELECT
    closed_at_year,
    closed_at_year_filled,
    MAX(first_closed_at) AS last_first_closed_at,
    COUNT(*) AS n_rows
FROM scored
GROUP BY CUBE(closed_at_year, closed_at_year_filled)
ORDER BY closed_at_year NULLS LAST, closed_at_year_filled NULLS LAST
