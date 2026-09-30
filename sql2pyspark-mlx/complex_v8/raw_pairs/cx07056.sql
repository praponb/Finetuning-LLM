WITH valid AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        UPPER(TRIM(zone)) AS zone_clean
    FROM sites
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket,
        ROUND(installed_kw * 0.9, 2) AS installed_kw_adj
    FROM valid
    WHERE tariff IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(installed_kw) OVER (PARTITION BY tariff, zone ORDER BY installed_kw_bucket NULLS LAST, id) AS prev_installed_kw,
        MAX(installed_kw_adj) OVER (PARTITION BY zone ORDER BY installed_kw NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS installed_kw_adj_max_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY zone ORDER BY installed_kw NULLS LAST), 3) AS installed_kw_percent_rank,
        ROUND(CUME_DIST() OVER w_zone_by_installed_kw_ties, 3) AS installed_kw_cume_dist
    FROM labeled
    WINDOW w_zone_by_installed_kw_ties AS (PARTITION BY zone ORDER BY installed_kw NULLS LAST)
),
derived AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(site_name), 1, 2) AS site_name_prefix
    FROM metrics
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        zone_clean,
        prev_installed_kw,
        installed_kw_adj_max_4,
        installed_kw_percent_rank,
        site_name_prefix,
        LAG(installed_kw_percent_rank, 2) OVER w_tariff_zone_by_prev_installe AS prev2_installed_kw_percent_rank,
        NTH_VALUE(prev_installed_kw, 2) OVER (PARTITION BY tariff, zone ORDER BY prev_installed_kw NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_installed_kw,
        SUM(installed_kw_percent_rank) OVER w_zone AS zone_sum_installed_kw_percent_rank
    FROM derived
    WINDOW w_tariff_zone_by_prev_installe AS (PARTITION BY tariff, zone ORDER BY prev_installed_kw NULLS LAST, id), w_zone AS (PARTITION BY zone)
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.id) AS reading_count
    FROM with_trends s
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM profiled s
    WHERE s.id IN (
            SELECT m.site_id
            FROM meter_readings m
        )
)
SELECT
    id,
    site_name,
    installed_kw,
    installed_kw_percent_rank,
    prev2_installed_kw_percent_rank,
    second_prev_installed_kw,
    zone_sum_installed_kw_percent_rank,
    reading_count,
    ROUND(second_prev_installed_kw * 1.5, 2) AS second_prev_installed_kw_adj
FROM filtered
WHERE prev2_installed_kw_percent_rank <= 0.6
ORDER BY second_prev_installed_kw_adj
