WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at,
        CASE WHEN requester_id >= 19 THEN 'large' WHEN requester_id >= 6 THEN 'medium' ELSE 'small' END AS requester_id_tier
    FROM tickets
    WHERE csat <= 4
),
per_group AS (
    -- Group-level totals
    SELECT
        TO_DATE(opened_at) AS opened_at_day,
        TO_DATE(closed_at) AS closed_at_day,
        COUNT(*) AS n_rows,
        MAX(hours_opened_at_to_closed_at) AS max_hours_opened_at_to_closed_at,
        SUM(CASE WHEN priority IS NOT NULL THEN 1 ELSE 0 END) AS n_priority_is_not_null,
        ROUND(STDDEV(csat), 2) AS stddev_csat,
        SUM(csat) AS total_csat,
        COUNT(DISTINCT channel) AS distinct_channels
    FROM base
    WHERE csat > 2
    GROUP BY TO_DATE(opened_at), TO_DATE(closed_at)
    HAVING COUNT(*) >= 2
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        opened_at_day,
        closed_at_day,
        n_rows,
        max_hours_opened_at_to_closed_at,
        n_priority_is_not_null,
        total_csat,
        NTILE(4) OVER (PARTITION BY distinct_channels, YEAR(opened_at_day) ORDER BY n_rows, opened_at_day, closed_at_day) AS n_rows_quartile,
        COUNT(*) OVER (PARTITION BY MONTH(opened_at_day), distinct_channels) AS rows_in_opened_at_day_month_distinct_channels
    FROM per_group
),
tagged AS (
    -- Derive helper columns
    SELECT
        opened_at_day,
        closed_at_day,
        n_rows,
        max_hours_opened_at_to_closed_at,
        n_priority_is_not_null,
        total_csat,
        n_rows_quartile,
        rows_in_opened_at_day_month_distinct_channels,
        DATEDIFF(DATE '2025-06-30', opened_at_day) AS days_since_opened_at_day
    FROM metrics
)
SELECT opened_at_day, closed_at_day, metric, amount
FROM (
    SELECT opened_at_day, closed_at_day, n_rows_quartile, n_rows, days_since_opened_at_day
    FROM tagged
) src
UNPIVOT (
    amount FOR metric IN (n_rows_quartile, n_rows, days_since_opened_at_day)
) unp
ORDER BY opened_at_day, closed_at_day, metric
