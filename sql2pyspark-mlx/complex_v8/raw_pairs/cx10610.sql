WITH valid AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        CONCAT(site_name, '#', CAST(id AS STRING)) AS site_name_ref,
        QUARTER(commissioned_on) AS commissioned_on_quarter
    FROM sites
    WHERE zone <> 'Z4'
      AND installed_kw >= 93.3
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        commissioned_on,
        site_name_ref,
        commissioned_on_quarter,
        DENSE_RANK() OVER (PARTITION BY zone ORDER BY commissioned_on_quarter NULLS LAST, id) AS commissioned_on_quarter_dense_rank,
        LEAD(installed_kw) OVER (PARTITION BY tariff ORDER BY commissioned_on, id) AS next_installed_kw,
        COUNT(*) OVER (PARTITION BY tariff ORDER BY commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(zone) OVER (PARTITION BY commissioned_on_quarter, tariff)) AS distinct_zone_in_commissioned_on_quarter_tariff,
        RANK() OVER (ORDER BY commissioned_on DESC NULLS FIRST) AS commissioned_on_rank,
        LEAD(commissioned_on_quarter) OVER (PARTITION BY zone ORDER BY commissioned_on_quarter NULLS LAST, id) AS next_commissioned_on_quarter
    FROM valid
),
zone_stats AS (
    -- Max commissioned_on_rank per zone
    SELECT zone, MAX(commissioned_on_rank) AS max_commissioned_on_rank
    FROM with_windows
    GROUP BY zone
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_commissioned_on_rank,
        ROUND(s.commissioned_on_rank - g.max_commissioned_on_rank, 2) AS commissioned_on_rank_minus_max
    FROM with_windows s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.commissioned_on_rank = g.max_commissioned_on_rank
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.id) AS first_reading_read_at
    FROM vs_group s
)
SELECT
    id,
    site_name,
    installed_kw,
    site_name_ref,
    commissioned_on_quarter,
    next_installed_kw,
    running_count,
    distinct_zone_in_commissioned_on_quarter_tariff,
    commissioned_on_rank,
    next_commissioned_on_quarter,
    commissioned_on_rank_minus_max,
    first_reading_read_at
FROM profiled
ORDER BY commissioned_on_rank NULLS LAST
