WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        FLOOR(voltage / 5) * 5 AS voltage_bucket_2,
        read_at + INTERVAL 30 MINUTES AS read_at_plus_30m
    FROM meter_readings
    WHERE (site_id IS NOT NULL OR quality IN ('BAD', 'EST'))
),
quality_stats AS (
    -- Median voltage_bucket per quality
    SELECT quality, PERCENTILE_APPROX(voltage_bucket, 0.5) AS median_voltage_bucket
    FROM valid_readings
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_voltage_bucket,
        ROUND(s.voltage_bucket - g.median_voltage_bucket, 2) AS voltage_bucket_minus_median
    FROM valid_readings s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.voltage_bucket >= g.median_voltage_bucket
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        site_id,
        MAX(voltage_bucket_2) AS max_voltage_bucket_2,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        COUNT(*) AS n_rows,
        MIN(kwh) AS min_kwh
    FROM above_average
    GROUP BY DATE_FORMAT(read_at, 'yyyy-MM'), site_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        read_at_ym,
        site_id,
        max_voltage_bucket_2,
        quality_list,
        n_rows,
        LOWER(REGEXP_REPLACE(TRIM(read_at_ym), ' ', '_')) AS read_at_ym_slug
    FROM rollup_base
    WHERE n_rows BETWEEN 4 AND 9
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(n_rows) OVER (ORDER BY max_voltage_bucket_2 ASC, read_at_ym, site_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS n_rows_sum_7,
        ROW_NUMBER() OVER (ORDER BY max_voltage_bucket_2 ASC, read_at_ym, site_id) AS seq_no
    FROM enriched
)
SELECT read_at_ym, site_id, field, amount
FROM (
    SELECT read_at_ym, site_id, n_rows_sum_7, max_voltage_bucket_2, n_rows, seq_no
    FROM sequenced
) src
UNPIVOT (
    amount FOR field IN (n_rows_sum_7, max_voltage_bucket_2, n_rows, seq_no)
) unp
