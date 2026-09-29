WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label
    FROM meter_readings
    WHERE voltage < 229.1
      AND site_id <= 8
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.quality_or_unknown,
        s.read_at_label,
        si.tariff AS site_tariff
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff = 'FLAT'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        quality_or_unknown,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id ORDER BY read_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_2_prior,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_tariff), 0), 4) AS kwh_share_of_site_tariff,
        SIZE(COLLECT_SET(quality_or_unknown) OVER (PARTITION BY site_tariff)) AS distinct_quality_or_unknown_in_site_tariff,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_tariff), 2) AS kwh_vs_site_tariff_avg,
        LAST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh
    FROM joined
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality_or_unknown,
        s.distinct_quality_or_unknown_in_site_tariff,
        s.kwh_vs_site_tariff_avg,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM metrics s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.read_at < TIMESTAMP '2025-02-12 12:00:00'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(reading_voltage / NULLIF(SUM(reading_voltage) OVER w_distinct_quality_or_unknown, 0), 4) AS reading_voltage_share_of_distinct_quality_or_unk,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY distinct_quality_or_unknown_in_site_tariff), 0), 4) AS kwh_share_of_distinct_quality_or_unknown_in_site,
        NTILE(4) OVER (PARTITION BY reading_quality ORDER BY reading_read_at ASC, id, reading_id) AS reading_read_at_quartile
    FROM detail_rows_readings
    WINDOW w_distinct_quality_or_unknown AS (PARTITION BY distinct_quality_or_unknown_in_site_tariff)
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality_or_unknown,
        distinct_quality_or_unknown_in_site_tariff,
        kwh_vs_site_tariff_avg,
        reading_id,
        reading_voltage,
        reading_read_at,
        reading_quality,
        reading_voltage_share_of_distinct_quality_or_unk,
        kwh_share_of_distinct_quality_or_unknown_in_site,
        reading_read_at_quartile,
        LAG(read_at, 3) OVER w_quality_or_unknown_by_readin AS prev3_read_at,
        DENSE_RANK() OVER (PARTITION BY reading_quality, distinct_quality_or_unknown_in_site_tariff ORDER BY reading_voltage_share_of_distinct_quality_or_unk, id, reading_id) AS reading_voltage_share_of_distinct_quality_or_unk_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY reading_quality, distinct_quality_or_unknown_in_site_tariff ORDER BY reading_voltage_share_of_distinct_quality_or_unk), 3) AS reading_voltage_share_of_distinct_quality_or_unk_3
    FROM windowed
    WINDOW w_quality_or_unknown_by_readin AS (PARTITION BY quality_or_unknown ORDER BY reading_read_at DESC, id, reading_id)
),
totals AS (
    -- Summarise per group
    SELECT
        distinct_quality_or_unknown_in_site_tariff,
        COUNT(*) AS row_count,
        ROUND(STDDEV(kwh_share_of_distinct_quality_or_unknown_in_site), 2) AS stddev_kwh_share_of_distinct_quality_or_unknown_
    FROM windowed_2
    WHERE site_id <= 2
    GROUP BY distinct_quality_or_unknown_in_site_tariff
)
SELECT
    distinct_quality_or_unknown_in_site_tariff,
    stddev_kwh_share_of_distinct_quality_or_unknown_,
    ABS(row_count - 8) AS row_count_distance,
    COALESCE(row_count, 0) AS row_count_filled
FROM totals
WHERE distinct_quality_or_unknown_in_site_tariff < 8
