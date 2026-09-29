WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        YEAR(read_at) AS read_at_year,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE (quality = 'OK' OR kwh <= 90.6)
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_year,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on
    FROM base s
    JOIN sites si ON si.id = s.site_id
    WHERE YEAR(si.commissioned_on) = 2025
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_tariff), 2) AS voltage_vs_site_tariff_avg,
        SUM(read_at_year) OVER (PARTITION BY site_id ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_read_at_year,
        RANK() OVER (ORDER BY site_commissioned_on NULLS LAST) AS site_commissioned_on_rank,
        MAX(voltage) OVER (ORDER BY site_commissioned_on NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS voltage_max_4,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality), 0), 4) AS voltage_share_of_quality,
        NTILE(3) OVER w_site_id_by_site_commissioned_2 AS site_commissioned_on_tercile,
        LEAD(site_commissioned_on, 2) OVER (PARTITION BY site_id, quality ORDER BY read_at NULLS LAST, id) AS next2_site_commissioned_on,
        MAX(voltage) OVER (PARTITION BY site_id ORDER BY site_commissioned_on DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS voltage_max_4_2,
        NTILE(4) OVER (PARTITION BY site_id, quality ORDER BY read_at NULLS LAST, id) AS read_at_quartile
    FROM with_parent_site
    WINDOW w_site_id_by_site_commissioned_2 AS (PARTITION BY site_id ORDER BY site_commissioned_on DESC, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY site_commissioned_on_rank, read_at_quartile ORDER BY voltage DESC), 3) AS voltage_cume_dist,
        FIRST_VALUE(site_commissioned_on_tercile) OVER (PARTITION BY site_commissioned_on_rank, read_at_quartile ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_site_commissioned_on_tercile,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY site_commissioned_on_rank, quality)) AS distinct_site_tariff_in_site_commissioned_on_ran,
        ROUND(AVG(read_at_year) OVER (PARTITION BY site_commissioned_on_rank, read_at_quartile ORDER BY voltage DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS read_at_year_avg_2_prior,
        NTILE(3) OVER w_quality_site_commissioned_on AS running_read_at_year_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality, MONTH(site_commissioned_on) ORDER BY running_read_at_year ASC, id), 3) AS running_read_at_year_percent_rank,
        ROW_NUMBER() OVER w_site_commissioned_on_rank_re_4 AS voltage_seq,
        LAST_VALUE(site_commissioned_on_rank) IGNORE NULLS OVER (PARTITION BY site_commissioned_on_rank, read_at_quartile ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_site_commissioned_on_rank
    FROM windowed
    WINDOW w_quality_site_commissioned_on AS (PARTITION BY quality, MONTH(site_commissioned_on) ORDER BY running_read_at_year ASC, id), w_site_commissioned_on_rank_re_4 AS (PARTITION BY site_commissioned_on_rank, read_at_quartile ORDER BY voltage DESC, id)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    read_at_year,
    site_tariff,
    site_commissioned_on,
    voltage_vs_site_tariff_avg,
    running_read_at_year,
    site_commissioned_on_rank,
    voltage_max_4,
    site_commissioned_on_tercile,
    next2_site_commissioned_on,
    read_at_quartile,
    voltage_cume_dist,
    first_site_commissioned_on_tercile,
    distinct_site_tariff_in_site_commissioned_on_ran,
    read_at_year_avg_2_prior,
    running_read_at_year_percent_rank,
    voltage_seq,
    TO_DATE(read_at) AS read_at_date,
    INITCAP(TRIM(quality)) AS quality_clean
FROM with_windows
WHERE first_site_commissioned_on_tercile < 7
ORDER BY site_id DESC
