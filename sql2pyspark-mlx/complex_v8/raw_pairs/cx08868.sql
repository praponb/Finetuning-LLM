WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 10 AND 12
),
with_details_site AS (
    -- Join sites
    SELECT
        s.*,
        si.site_name,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff
    FROM cleaned_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.days_since_read_at,
        s.quality_or_unknown,
        s.site_name,
        s.site_installed_kw,
        s.site_tariff,
        si.commissioned_on AS site_commissioned_on
    FROM with_details_site s
    JOIN sites si ON si.id = s.site_id AND si.zone = 'Z3'
    WHERE si.commissioned_on < DATE '2024-11-21'
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_voltage,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM enriched_site s
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        days_since_read_at,
        quality_or_unknown,
        site_name,
        site_installed_kw,
        site_tariff,
        site_commissioned_on,
        total_reading_voltage,
        RANK() OVER w_quality_or_unknown_by_site_i AS site_installed_kw_rank,
        DENSE_RANK() OVER w_site_id_read_at_year_by_days AS days_since_read_at_dense_rank,
        LAST_VALUE(site_installed_kw) OVER (PARTITION BY quality_or_unknown ORDER BY site_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_installed_kw,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality_or_unknown ORDER BY read_at ASC, id), 3) AS read_at_cume_dist,
        LAG(total_reading_voltage) OVER (PARTITION BY site_id, quality_or_unknown ORDER BY read_at ASC, id) AS prev_total_reading_voltage,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER (PARTITION BY quality_or_unknown, site_tariff), 0), 4) AS site_installed_kw_share_of_quality_or_unknown_si
    FROM profiled
    WINDOW w_quality_or_unknown_by_site_i AS (PARTITION BY quality_or_unknown ORDER BY site_installed_kw), w_site_id_read_at_year_by_days AS (PARTITION BY site_id, YEAR(read_at) ORDER BY days_since_read_at DESC NULLS FIRST, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        site_name,
        site_tariff,
        site_commissioned_on,
        total_reading_voltage,
        site_installed_kw_rank,
        days_since_read_at_dense_rank,
        read_at_cume_dist,
        prev_total_reading_voltage,
        site_installed_kw_share_of_quality_or_unknown_si,
        ROUND(AVG(site_installed_kw_rank) OVER (ORDER BY site_installed_kw, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS site_installed_kw_rank_avg_1_prior,
        ROUND(total_reading_voltage - AVG(total_reading_voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on)), 2) AS total_reading_voltage_vs_site_commissioned_on_mo,
        FIRST_VALUE(read_at_cume_dist) OVER (ORDER BY site_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at_cume_dist
    FROM windowed
)
SELECT
    id,
    read_at,
    site_name,
    site_tariff,
    total_reading_voltage,
    site_installed_kw_rank,
    days_since_read_at_dense_rank,
    read_at_cume_dist,
    prev_total_reading_voltage,
    site_installed_kw_share_of_quality_or_unknown_si,
    site_installed_kw_rank_avg_1_prior,
    total_reading_voltage_vs_site_commissioned_on_mo,
    first_read_at_cume_dist
FROM metrics
ORDER BY read_at NULLS LAST, site_name, id
LIMIT 20
