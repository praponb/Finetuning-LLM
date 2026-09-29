WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE kwh >= 35.0
),
with_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        si.site_name,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM valid s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on < DATE '2024-11-21'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(voltage) OVER (PARTITION BY site_id, MONTH(site_commissioned_on)) AS site_id_site_commissioned_on_month_min_voltage,
        RANK() OVER w_all_by_site_installed_kw AS site_installed_kw_rank,
        NTILE(4) OVER (ORDER BY site_installed_kw, id) AS site_installed_kw_quartile,
        NTILE(3) OVER (ORDER BY site_installed_kw, id) AS site_installed_kw_tercile,
        COUNT(*) OVER (PARTITION BY site_id, MONTH(site_commissioned_on)) AS rows_in_site_id_site_commissioned_on_month,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY site_installed_kw DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage,
        ROW_NUMBER() OVER (ORDER BY site_installed_kw, id) AS seq_no,
        MAX(voltage) OVER (PARTITION BY site_id, MONTH(site_commissioned_on)) AS site_id_site_commissioned_on_month_max_voltage,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_site_id_site_commissioned_on, 0), 4) AS voltage_share_of_site_id_site_commissioned_on_mo
    FROM with_details
    WINDOW w_all_by_site_installed_kw AS (ORDER BY site_installed_kw, id), w_site_id_site_commissioned_on AS (PARTITION BY site_id, MONTH(site_commissioned_on))
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        site_name,
        site_installed_kw,
        site_commissioned_on,
        site_id_site_commissioned_on_month_min_voltage,
        site_installed_kw_rank,
        site_installed_kw_quartile,
        site_installed_kw_tercile,
        rows_in_site_id_site_commissioned_on_month,
        first_known_voltage,
        site_id_site_commissioned_on_month_max_voltage,
        voltage_share_of_site_id_site_commissioned_on_mo
    FROM with_windows
    WHERE seq_no <= 5
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        site_installed_kw_quartile,
        CASE WHEN kwh >= 109.0 THEN 'HIGH' WHEN kwh >= 29.2 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier,
        site_installed_kw_rank,
        ROUND(SUM(kwh), 2) AS total_kwh,
        SUM(CASE WHEN UPPER(TRIM(site_name)) LIKE 'H%' THEN site_id_site_commissioned_on_month_max_voltage ELSE 0 END) AS site_id_site_commissioned_on_month_max_voltage_u,
        PERCENTILE_APPROX(site_installed_kw_rank, 0.5) AS median_site_installed_kw_rank,
        SUM(CASE WHEN LOWER(site_name) LIKE '%ot%' THEN site_id_site_commissioned_on_month_min_voltage ELSE 0 END) AS site_id_site_commissioned_on_month_min_voltage_l
    FROM first_rows
    GROUP BY 1, 2, 3
    HAVING SUM(voltage_share_of_site_id_site_commissioned_on_mo) > 0.2
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        site_installed_kw_quartile,
        kwh_tier,
        site_installed_kw_rank,
        total_kwh,
        site_id_site_commissioned_on_month_max_voltage_u,
        median_site_installed_kw_rank,
        site_id_site_commissioned_on_month_min_voltage_l,
        ROUND(total_kwh / NULLIF(SUM(total_kwh) OVER (PARTITION BY site_installed_kw_rank), 0), 4) AS total_kwh_share_of_site_installed_kw_rank,
        ROUND(site_installed_kw_rank / NULLIF(SUM(site_installed_kw_rank) OVER (PARTITION BY site_installed_kw_rank), 0), 4) AS site_installed_kw_rank_share_of_site_installed_k
    FROM grouped
)
-- Final projection
SELECT
    site_installed_kw_quartile,
    kwh_tier,
    site_installed_kw_rank,
    total_kwh,
    site_id_site_commissioned_on_month_max_voltage_u,
    median_site_installed_kw_rank,
    site_id_site_commissioned_on_month_min_voltage_l,
    total_kwh_share_of_site_installed_kw_rank,
    site_installed_kw_rank_share_of_site_installed_k,
    LENGTH(TRIM(kwh_tier)) AS kwh_tier_len
FROM ranked
ORDER BY total_kwh_share_of_site_installed_kw_rank
