WITH cleaned_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE site_id >= 3
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        COUNT(*) AS row_count,
        MIN(kwh) AS min_kwh,
        MAX(read_at) AS last_read_at,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM cleaned_readings
    GROUP BY DATE_TRUNC('MONTH', read_at)
    HAVING MAX(days_since_read_at) > 70
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(row_count) OVER w_read_at_month_month AS read_at_month_month_max_row_count,
        ROUND(CUME_DIST() OVER (PARTITION BY row_count ORDER BY min_kwh), 3) AS min_kwh_cume_dist,
        SUM(row_count) OVER (PARTITION BY DATE_TRUNC('MONTH', last_read_at) ORDER BY last_read_at ASC, read_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_count,
        NTILE(4) OVER w_last_read_at_month_by_last_r_2 AS last_read_at_quartile,
        COUNT(*) OVER w_read_at_month_month AS rows_in_read_at_month_month
    FROM summary
    WINDOW w_read_at_month_month AS (PARTITION BY MONTH(read_at_month)), w_last_read_at_month_by_last_r_2 AS (PARTITION BY DATE_TRUNC('MONTH', last_read_at) ORDER BY last_read_at ASC, read_at_month)
)
-- Final projection
SELECT
    read_at_month,
    row_count,
    last_read_at,
    read_at_month_month_max_row_count,
    running_row_count,
    rows_in_read_at_month_month,
    DATE_FORMAT(last_read_at, 'MMM yyyy') AS last_read_at_label,
    ROUND(min_kwh * 0.25, 2) AS min_kwh_adj
FROM ranked
WHERE last_read_at_quartile > 3
ORDER BY running_row_count DESC
