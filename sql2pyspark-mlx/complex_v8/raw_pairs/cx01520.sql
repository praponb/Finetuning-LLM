WITH recent_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        commissioned_on,
        CONCAT_WS('-', site_name, zone) AS site_name_zone_label,
        DATE_ADD(commissioned_on, 7) AS commissioned_on_plus_7d
    FROM sites
    WHERE installed_kw BETWEEN 96.9 AND 446.9
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM recent_sites s
    WHERE s.id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.kwh > 45.0
              AND HOUR(m.read_at) BETWEEN 10 AND 14
        )
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(installed_kw) OVER (ORDER BY installed_kw, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS installed_kw_avg_1_prior,
        DENSE_RANK() OVER (ORDER BY installed_kw, id) AS installed_kw_dense_rank,
        ROUND(AVG(installed_kw) OVER (ORDER BY installed_kw, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS installed_kw_avg_7,
        LAG(installed_kw, 3, 0) OVER (ORDER BY installed_kw, id) AS prev3_installed_kw
    FROM qualified
),
latest_per_key AS (
    -- Keep the latest row per zone
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        commissioned_on,
        site_name_zone_label,
        commissioned_on_plus_7d,
        installed_kw_avg_1_prior,
        installed_kw_dense_rank,
        installed_kw_avg_7,
        prev3_installed_kw
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone ORDER BY installed_kw DESC, id DESC) AS recency_rank
        FROM sequenced
    ) ranked
    WHERE recency_rank = 1
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.first_reading_read_at
    FROM latest_per_key s
    LEFT JOIN reading_stats m ON m.site_id = s.id
)
-- Final projection
SELECT
    id,
    site_name,
    zone,
    installed_kw,
    commissioned_on,
    site_name_zone_label,
    commissioned_on_plus_7d,
    installed_kw_avg_1_prior,
    installed_kw_dense_rank,
    reading_count,
    first_reading_read_at
FROM with_stats
ORDER BY reading_count DESC, commissioned_on NULLS LAST
