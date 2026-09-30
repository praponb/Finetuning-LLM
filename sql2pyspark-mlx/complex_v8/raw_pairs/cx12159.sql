WITH recent_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        UPPER(TRIM(site_name)) AS site_name_clean,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        CONCAT(site_name, '#', CAST(id AS STRING)) AS site_name_ref
    FROM sites
    WHERE LENGTH(TRIM(site_name)) >= 4
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.id) AS last_reading_read_at,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.id) AS reading_count
    FROM recent_sites s
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.quality = 'OK'
              AND m.voltage > 222.6
        )
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered
    WHERE reading_count > (SELECT AVG(reading_count) FROM filtered)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY reading_count ORDER BY reading_count, id) AS reading_count_dense_rank,
        RANK() OVER (PARTITION BY reading_count ORDER BY reading_count) AS reading_count_rank,
        ROUND(reading_count - AVG(reading_count) OVER (PARTITION BY YEAR(last_reading_read_at)), 2) AS reading_count_vs_last_reading_read_at_year_avg,
        DENSE_RANK() OVER (PARTITION BY reading_count ORDER BY reading_count) AS reading_count_dense_rank_2
    FROM qualified
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(tariff_or_unknown) OVER (PARTITION BY tariff, reading_count_dense_rank_2)) AS distinct_tariff_or_unknown_in_tariff_reading_cou,
        NTILE(4) OVER (PARTITION BY reading_count_dense_rank_2, zone ORDER BY reading_count DESC, id) AS reading_count_quartile,
        ROUND(PERCENT_RANK() OVER (ORDER BY reading_count NULLS LAST), 3) AS reading_count_percent_rank,
        LAG(last_reading_read_at, 2) OVER w_reading_count_dense_rank_2_z AS prev2_last_reading_read_at
    FROM with_windows
    WINDOW w_reading_count_dense_rank_2_z AS (PARTITION BY reading_count_dense_rank_2, zone ORDER BY reading_count DESC, id)
)
-- Final projection
SELECT
    id,
    tariff,
    site_name_clean,
    tariff_or_unknown,
    site_name_ref,
    reading_count,
    reading_count_vs_last_reading_read_at_year_avg,
    reading_count_dense_rank_2,
    distinct_tariff_or_unknown_in_tariff_reading_cou,
    reading_count_quartile,
    reading_count_percent_rank,
    CONCAT_WS(': ', site_name, tariff) AS site_name_tariff_label,
    QUARTER(last_reading_read_at) AS last_reading_read_at_quarter
FROM ranked
ORDER BY reading_count_dense_rank_2 NULLS LAST, last_reading_read_at_quarter DESC
