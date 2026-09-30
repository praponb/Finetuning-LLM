WITH scoped_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        tariff,
        installed_kw,
        commissioned_on,
        COALESCE(installed_kw, 0.0) AS installed_kw_filled,
        ABS(installed_kw - 141.6) AS installed_kw_distance,
        LENGTH(TRIM(site_name)) AS site_name_len
    FROM sites
    WHERE (UPPER(TRIM(site_name)) LIKE 'H%' OR zone = 'Z2')
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        site_name,
        installed_kw,
        commissioned_on,
        installed_kw_distance,
        site_name_len,
        installed_kw_filled + installed_kw AS installed_kw_filled_plus_installed_kw,
        installed_kw_distance - installed_kw AS installed_kw_distance_minus_installed_kw,
        ABS(installed_kw_filled - 203.0) AS installed_kw_filled_distance
    FROM scoped_sites
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        site_name,
        commissioned_on,
        site_name_len,
        installed_kw_distance_minus_installed_kw,
        ROUND(site_name_len / NULLIF(installed_kw, 0), 3) AS site_name_len_per_installed_kw,
        DATE_FORMAT(commissioned_on, 'MMM yyyy') AS commissioned_on_label
    FROM annotated
    WHERE installed_kw_filled_plus_installed_kw <= 651.8
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        commissioned_on,
        site_name_len,
        site_name_len_per_installed_kw,
        commissioned_on_label,
        LAG(commissioned_on, 3) OVER (PARTITION BY site_name_len, YEAR(commissioned_on) ORDER BY installed_kw_distance_minus_installed_kw DESC, id) AS prev3_commissioned_on,
        DENSE_RANK() OVER (ORDER BY installed_kw_distance_minus_installed_kw DESC NULLS FIRST, id) AS installed_kw_distance_minus_installed_kw_dense_r,
        FIRST_VALUE(site_name_len) OVER (ORDER BY installed_kw_distance_minus_installed_kw DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_site_name_len,
        SUM(site_name_len) OVER (PARTITION BY site_name_len, YEAR(commissioned_on) ORDER BY installed_kw_distance_minus_installed_kw DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS site_name_len_sum_3,
        ROUND(PERCENT_RANK() OVER w_site_name_len_commissioned_o_3, 3) AS installed_kw_distance_minus_installed_kw_percent
    FROM derived
    WINDOW w_site_name_len_commissioned_o_3 AS (PARTITION BY site_name_len, YEAR(commissioned_on) ORDER BY installed_kw_distance_minus_installed_kw DESC)
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
        )
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_name_len_per_installed_kw,
        s.prev3_commissioned_on,
        s.installed_kw_distance_minus_installed_kw_dense_r,
        s.first_site_name_len,
        s.installed_kw_distance_minus_installed_kw_percent,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM filtered s
    JOIN meter_readings m ON m.site_id = s.id AND m.voltage < 231.7
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_name_len_per_installed_kw,
        prev3_commissioned_on,
        installed_kw_distance_minus_installed_kw_dense_r,
        installed_kw_distance_minus_installed_kw_percent,
        reading_id,
        reading_voltage,
        reading_kwh,
        SUM(first_site_name_len) OVER (ORDER BY first_site_name_len DESC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_first_site_name_len,
        MIN(reading_kwh) OVER (PARTITION BY YEAR(prev3_commissioned_on) ORDER BY site_name_len_per_installed_kw, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_reading_kwh_so_far
    FROM detail_rows_readings
)
-- Final projection
SELECT
    id,
    site_name_len_per_installed_kw,
    installed_kw_distance_minus_installed_kw_dense_r,
    installed_kw_distance_minus_installed_kw_percent,
    reading_id,
    reading_voltage,
    reading_kwh,
    running_first_site_name_len,
    min_reading_kwh_so_far,
    LAST_DAY(prev3_commissioned_on) AS prev3_commissioned_on_month_end
FROM with_windows
WHERE installed_kw_distance_minus_installed_kw_dense_r < 8
