WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.days_since_read_at,
        s.quality_len,
        si.commissioned_on AS site_commissioned_on,
        si.site_name,
        si.installed_kw AS site_installed_kw
    FROM filtered s
    JOIN sites si ON si.id = s.site_id AND si.zone NOT IN ('Z3', 'Z2')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        days_since_read_at,
        quality_len,
        site_installed_kw,
        ROUND(PERCENT_RANK() OVER (ORDER BY quality_len ASC), 3) AS quality_len_percent_rank,
        LEAD(voltage, 2, 0) OVER (PARTITION BY MONTH(site_commissioned_on) ORDER BY days_since_read_at DESC, id) AS next2_voltage,
        MIN(voltage) OVER (PARTITION BY site_id) AS site_id_min_voltage,
        RANK() OVER (PARTITION BY quality_len, YEAR(site_commissioned_on) ORDER BY site_installed_kw ASC, id) AS site_installed_kw_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY quality_len ASC, id), 3) AS quality_len_percent_rank_2,
        ROUND(AVG(site_installed_kw) OVER w_quality_len_site_id, 2) AS quality_len_site_id_avg_site_installed_kw,
        LAG(site_installed_kw, 2) OVER (PARTITION BY MONTH(site_commissioned_on) ORDER BY days_since_read_at DESC, id) AS prev2_site_installed_kw,
        RANK() OVER (PARTITION BY quality_len ORDER BY days_since_read_at DESC, id) AS days_since_read_at_rank
    FROM enriched
    WINDOW w_quality_len_site_id AS (PARTITION BY quality_len, site_id)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM metrics
    WHERE site_installed_kw_rank <= 5
      AND prev2_site_installed_kw > 96.9
)
SELECT
    id,
    site_id,
    quality_len,
    site_installed_kw,
    quality_len_percent_rank,
    next2_voltage,
    site_installed_kw_rank,
    quality_len_percent_rank_2,
    quality_len_site_id_avg_site_installed_kw,
    prev2_site_installed_kw,
    days_since_read_at_rank
FROM top_rows
