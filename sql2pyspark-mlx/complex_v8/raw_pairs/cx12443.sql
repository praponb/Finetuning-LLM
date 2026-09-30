WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 0.9, 2) AS kwh_adj,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE voltage >= 215.6
),
joined_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.site_name
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
    WHERE si.site_name IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        voltage,
        quality,
        kwh_adj,
        days_since_read_at,
        site_name,
        ROUND(days_since_read_at - AVG(days_since_read_at) OVER (PARTITION BY TO_DATE(read_at), site_id), 2) AS days_since_read_at_vs_read_at_to_date_site_id_av,
        LAG(read_at) OVER w_quality_by_kwh_adj AS prev_read_at,
        ROW_NUMBER() OVER (ORDER BY days_since_read_at, id) AS days_since_read_at_seq,
        SUM(days_since_read_at) OVER (PARTITION BY quality ORDER BY kwh_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_read_at,
        MIN(kwh) OVER (PARTITION BY quality, site_id) AS quality_site_id_min_kwh,
        MAX(days_since_read_at) OVER (PARTITION BY YEAR(read_at) ORDER BY kwh NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS days_since_read_at_max_2,
        LEAD(days_since_read_at, 1, 0) OVER (PARTITION BY quality ORDER BY kwh_adj DESC, id) AS next_days_since_read_at,
        ROUND(CUME_DIST() OVER w_read_at_year_by_kwh, 3) AS kwh_cume_dist,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY TO_DATE(read_at), site_id)) AS distinct_quality_in_read_at_to_date_site_id
    FROM joined_site
    WINDOW w_quality_by_kwh_adj AS (PARTITION BY quality ORDER BY kwh_adj DESC, id), w_read_at_year_by_kwh AS (PARTITION BY YEAR(read_at) ORDER BY kwh NULLS LAST, id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(days_since_read_at / NULLIF(voltage, 0), 3) AS days_since_read_at_per_voltage
    FROM ranked
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        quality,
        kwh_adj,
        days_since_read_at,
        site_name,
        days_since_read_at_vs_read_at_to_date_site_id_av,
        prev_read_at,
        days_since_read_at_seq,
        quality_site_id_min_kwh,
        days_since_read_at_max_2,
        kwh_cume_dist,
        COUNT(*) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', prev_read_at)) AS rows_in_site_id_prev_read_at_month,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', prev_read_at))) AS distinct_quality_in_site_id_prev_read_at_month,
        MAX(days_since_read_at) OVER (PARTITION BY site_id ORDER BY days_since_read_at_seq NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS days_since_read_at_max_2_2,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(prev_read_at), site_id ORDER BY days_since_read_at_seq DESC, id), 3) AS days_since_read_at_seq_cume_dist,
        ROUND(kwh_cume_dist / NULLIF(SUM(kwh_cume_dist) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', prev_read_at)), 0), 4) AS kwh_cume_dist_share_of_site_id_prev_read_at_mont
    FROM tagged
)
SELECT
    id,
    site_id,
    quality,
    kwh_adj,
    days_since_read_at,
    site_name,
    days_since_read_at_vs_read_at_to_date_site_id_av,
    prev_read_at,
    days_since_read_at_seq,
    quality_site_id_min_kwh,
    days_since_read_at_max_2,
    kwh_cume_dist,
    rows_in_site_id_prev_read_at_month,
    distinct_quality_in_site_id_prev_read_at_month,
    days_since_read_at_seq_cume_dist,
    kwh_cume_dist_share_of_site_id_prev_read_at_mont
FROM metrics
WHERE days_since_read_at > 140
ORDER BY days_since_read_at_vs_read_at_to_date_site_id_av DESC, prev_read_at DESC
