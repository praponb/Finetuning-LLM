WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled,
        INITCAP(TRIM(quality)) AS quality_clean,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE kwh >= 29.2
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(quality_or_unknown), 1, 2) AS quality_or_unknown_prefix
    FROM scoped_readings
),
latest_per_key AS (
    -- Keep the latest row per quality_or_unknown, site_id
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        kwh_filled,
        quality_clean,
        quality_or_unknown,
        quality_or_unknown_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality_or_unknown, site_id ORDER BY read_at DESC, id DESC) AS row_num
        FROM tagged
    ) ranked
    WHERE row_num = 1
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.kwh_filled,
        s.quality_or_unknown,
        s.quality_or_unknown_prefix,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM latest_per_key s
    JOIN meter_readings m ON m.site_id = s.site_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        reading_id,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        DATE_TRUNC('MONTH', reading_read_at) AS reading_read_at_month,
        ROUND(SUM(reading_kwh), 2) AS total_reading_kwh,
        COUNT(*) FILTER (WHERE quality IN ('OK', 'BAD')) AS n_quality_in_ok_bad
    FROM detail_rows_readings
    GROUP BY reading_id, DATE_TRUNC('MONTH', read_at), DATE_TRUNC('MONTH', reading_read_at)
    HAVING MAX(voltage) > 215.8
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        reading_id,
        read_at_month,
        reading_read_at_month,
        total_reading_kwh,
        n_quality_in_ok_bad,
        ROUND(total_reading_kwh / NULLIF(SUM(total_reading_kwh) OVER (PARTITION BY n_quality_in_ok_bad), 0), 4) AS total_reading_kwh_share_of_n_quality_in_ok_bad,
        ROUND(AVG(total_reading_kwh) OVER (PARTITION BY n_quality_in_ok_bad), 2) AS n_quality_in_ok_bad_avg_total_reading_kwh,
        FIRST_VALUE(reading_read_at_month) IGNORE NULLS OVER (PARTITION BY TO_DATE(read_at_month) ORDER BY n_quality_in_ok_bad DESC, reading_id, read_at_month, reading_read_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reading_read_at_month,
        ROUND(AVG(total_reading_kwh) OVER (PARTITION BY TO_DATE(read_at_month) ORDER BY n_quality_in_ok_bad DESC, reading_id, read_at_month, reading_read_at_month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS total_reading_kwh_avg_1_prior,
        COUNT(*) OVER (PARTITION BY n_quality_in_ok_bad) AS rows_in_n_quality_in_ok_bad
    FROM totals
)
-- Final projection
SELECT
    reading_id,
    read_at_month,
    reading_read_at_month,
    total_reading_kwh,
    n_quality_in_ok_bad,
    total_reading_kwh_share_of_n_quality_in_ok_bad,
    n_quality_in_ok_bad_avg_total_reading_kwh,
    first_known_reading_read_at_month,
    total_reading_kwh_avg_1_prior
FROM with_trends
ORDER BY reading_read_at_month DESC
