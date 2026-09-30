WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_label,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'outage')
      AND priority IS NOT NULL
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        opened_at,
        csat,
        closed_at_label,
        opened_at_month,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        UPPER(TRIM(closed_at_label)) AS closed_at_label_clean,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown_2
    FROM base_tickets
),
latest_per_key AS (
    -- Keep the latest row per agent_id, priority_or_unknown
    SELECT
        id,
        agent_id,
        opened_at,
        csat,
        closed_at_label,
        opened_at_month,
        priority_or_unknown,
        closed_at_label_clean,
        priority_or_unknown_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, priority_or_unknown ORDER BY opened_at DESC, id DESC) AS row_num
        FROM derived
    ) ranked
    WHERE row_num = 1
),
stats AS (
    -- Group-level totals
    SELECT
        agent_id,
        priority_or_unknown_2,
        PERCENTILE_APPROX(csat, 0.5) AS median_csat,
        MAX(opened_at) AS last_opened_at,
        COUNT(*) AS n_rows
    FROM latest_per_key
    GROUP BY agent_id, priority_or_unknown_2
    HAVING COUNT(*) >= 2
)
-- Native SQL UNPIVOT (wide to long)
SELECT agent_id, priority_or_unknown_2, field, amount
FROM (
    SELECT agent_id, priority_or_unknown_2, n_rows, median_csat
    FROM stats
) src
UNPIVOT (
    amount FOR field IN (n_rows, median_csat)
) unp
