WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE (HOUR(read_at) BETWEEN 6 AND 11 OR kwh > 51.0)
),
joined_site AS (
    -- Join sites
    SELECT
        s.*,
        si.site_name,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM scoped_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw <= 373.3
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(site_installed_kw) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_installed_kw,
        RANK() OVER w_read_at_month_by_read_at_tie AS read_at_rank,
        ROUND(AVG(days_since_read_at) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY site_commissioned_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS days_since_read_at_avg_2_prior,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY YEAR(read_at), MONTH(site_commissioned_on)), 2) AS voltage_vs_read_at_year_site_commissioned_on_mon,
        LEAD(days_since_read_at, 3, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY read_at, id) AS next3_days_since_read_at,
        ROUND(AVG(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY read_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_3_prior,
        SUM(site_installed_kw) OVER (PARTITION BY YEAR(read_at), MONTH(site_commissioned_on)) AS read_at_year_site_commissioned_on_month_sum_site,
        LAG(voltage) OVER w_read_at_month_by_read_at AS prev_voltage,
        SUM(days_since_read_at) OVER (PARTITION BY YEAR(read_at) ORDER BY days_since_read_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS days_since_read_at_sum_1_prior
    FROM joined_site
    WINDOW w_read_at_month_by_read_at_tie AS (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY read_at), w_read_at_month_by_read_at AS (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY read_at, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        days_since_read_at,
        site_name,
        read_at_rank,
        days_since_read_at_avg_2_prior,
        voltage_vs_read_at_year_site_commissioned_on_mon,
        next3_days_since_read_at,
        kwh_avg_3_prior,
        read_at_year_site_commissioned_on_month_sum_site,
        prev_voltage,
        days_since_read_at_sum_1_prior,
        LEAD(next3_days_since_read_at, 1, 0) OVER (PARTITION BY read_at_rank ORDER BY days_since_read_at_avg_2_prior, id) AS next_next3_days_since_read_at,
        LEAD(days_since_read_at, 3) OVER w_read_at_rank_quality_by_volt AS next3_days_since_read_at_2,
        COUNT(*) OVER w_read_at_rank_quality AS rows_in_read_at_rank_quality,
        ROUND(CUME_DIST() OVER (PARTITION BY read_at_rank ORDER BY days_since_read_at_avg_2_prior, id), 3) AS days_since_read_at_avg_2_prior_cume_dist,
        LEAD(read_at_rank, 1, 0) OVER (PARTITION BY read_at_rank, quality ORDER BY voltage DESC NULLS FIRST, id) AS next_read_at_rank,
        NTILE(5) OVER (PARTITION BY read_at_rank ORDER BY days_since_read_at_avg_2_prior, id) AS days_since_read_at_avg_2_prior_quintile,
        ROUND(kwh_avg_3_prior / NULLIF(SUM(kwh_avg_3_prior) OVER w_read_at_rank_quality, 0), 4) AS kwh_avg_3_prior_share_of_read_at_rank_quality
    FROM sequenced
    WINDOW w_read_at_rank_quality_by_volt AS (PARTITION BY read_at_rank, quality ORDER BY voltage DESC NULLS FIRST, id), w_read_at_rank_quality AS (PARTITION BY read_at_rank, quality)
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    quality,
    site_name,
    days_since_read_at_avg_2_prior,
    voltage_vs_read_at_year_site_commissioned_on_mon,
    next3_days_since_read_at,
    kwh_avg_3_prior,
    read_at_year_site_commissioned_on_month_sum_site,
    prev_voltage,
    days_since_read_at_sum_1_prior,
    next_next3_days_since_read_at,
    next3_days_since_read_at_2,
    rows_in_read_at_rank_quality,
    days_since_read_at_avg_2_prior_cume_dist,
    next_read_at_rank,
    days_since_read_at_avg_2_prior_quintile,
    INITCAP(TRIM(site_name)) AS site_name_clean,
    ROUND(kwh_avg_3_prior * 0.9, 2) AS kwh_avg_3_prior_adj
FROM with_trends
ORDER BY quality DESC, days_since_read_at_avg_2_prior_cume_dist NULLS LAST, id
LIMIT 20
