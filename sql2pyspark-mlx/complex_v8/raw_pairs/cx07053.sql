WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        zone,
        tariff,
        commissioned_on
    FROM sites
    WHERE tariff IS NOT NULL
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        zone,
        tariff,
        commissioned_on,
        DATEDIFF(DATE '2026-01-01', commissioned_on) AS days_since_commissioned_on,
        LENGTH(TRIM(zone)) AS zone_len
    FROM base
    WHERE commissioned_on IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(zone_len) OVER (PARTITION BY zone) AS zone_sum_zone_len,
        SIZE(COLLECT_SET(tariff) OVER (PARTITION BY zone)) AS distinct_tariff_in_zone,
        RANK() OVER (PARTITION BY zone ORDER BY commissioned_on NULLS LAST, id) AS commissioned_on_rank,
        DATEDIFF(commissioned_on, LAG(commissioned_on) OVER w_zone_len_by_commissioned_on) AS days_since_prev_commissioned_on,
        ROUND(CUME_DIST() OVER (PARTITION BY zone_len ORDER BY commissioned_on DESC, id), 3) AS commissioned_on_cume_dist,
        LEAD(days_since_commissioned_on) OVER (PARTITION BY zone_len ORDER BY commissioned_on DESC, id) AS next_days_since_commissioned_on
    FROM scored
    WINDOW w_zone_len_by_commissioned_on AS (PARTITION BY zone_len ORDER BY commissioned_on DESC, id)
),
tariff_stats AS (
    -- Median days_since_commissioned_on per tariff
    SELECT tariff, PERCENTILE_APPROX(days_since_commissioned_on, 0.5) AS median_days_since_commissioned_on
    FROM with_trends
    GROUP BY tariff
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_days_since_commissioned_on,
        ROUND(s.days_since_commissioned_on - g.median_days_since_commissioned_on, 2) AS days_since_commissioned_on_minus_median
    FROM with_trends s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.days_since_commissioned_on >= g.median_days_since_commissioned_on
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(read_at) AS last_reading_read_at,
        MAX(voltage) AS max_reading_voltage,
        COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at,
        m.max_reading_voltage,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM compared s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(commissioned_on_rank / NULLIF(distinct_tariff_in_zone, 0), 3) AS commissioned_on_rank_per_distinct_tariff_in_zone,
        LOWER(TRIM(zone)) AS zone_clean
    FROM summarized_rows
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(zone, 'UNKNOWN') AS zone_or_unknown,
        COALESCE(max_reading_voltage, 0.0) AS max_reading_voltage_filled,
        DATEDIFF(DATE '2025-12-31', commissioned_on) AS days_since_commissioned_on_2
    FROM derived
)
SELECT
    id,
    zone,
    tariff,
    commissioned_on,
    zone_len,
    zone_sum_zone_len,
    distinct_tariff_in_zone,
    commissioned_on_rank,
    next_days_since_commissioned_on,
    median_days_since_commissioned_on,
    days_since_commissioned_on_minus_median,
    last_reading_read_at,
    max_reading_voltage,
    commissioned_on_rank_per_distinct_tariff_in_zone,
    zone_clean,
    max_reading_voltage_filled,
    CONCAT(zone_or_unknown, '#', CAST(reading_count AS STRING)) AS zone_or_unknown_ref
FROM labeled
ORDER BY tariff NULLS LAST, median_days_since_commissioned_on
