WITH recent_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM sites
    WHERE commissioned_on >= DATE '2024-06-27'
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at
    FROM recent_sites s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(commissioned_on) OVER (PARTITION BY YEAR(first_reading_read_at) ORDER BY installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_commissioned_on,
        ROW_NUMBER() OVER (PARTITION BY YEAR(first_reading_read_at) ORDER BY installed_kw DESC, id) AS seq_no,
        LAST_VALUE(first_reading_read_at) IGNORE NULLS OVER (ORDER BY commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_reading_read_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(first_reading_read_at) ORDER BY installed_kw DESC, id), 3) AS installed_kw_percent_rank
    FROM with_stats
),
tariff_stats AS (
    -- Max installed_kw per tariff
    SELECT tariff, MAX(installed_kw) AS max_installed_kw
    FROM ranked
    GROUP BY tariff
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_installed_kw,
        ROUND(s.installed_kw - g.max_installed_kw, 2) AS installed_kw_minus_max
    FROM ranked s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.installed_kw = g.max_installed_kw
)
SELECT
    id,
    zone,
    installed_kw,
    first_reading_read_at,
    first_commissioned_on,
    seq_no,
    last_known_first_reading_read_at,
    installed_kw_percent_rank,
    max_installed_kw,
    installed_kw_minus_max,
    CASE WHEN max_installed_kw >= 425.8 THEN 'large' WHEN max_installed_kw >= 128.8 THEN 'medium' ELSE 'small' END AS max_installed_kw_tier,
    ROUND(seq_no / NULLIF(installed_kw, 0), 3) AS seq_no_per_installed_kw
FROM outliers
WHERE first_reading_read_at < TIMESTAMP '2025-02-09 09:00:00'
ORDER BY id DESC NULLS FIRST
LIMIT 5
