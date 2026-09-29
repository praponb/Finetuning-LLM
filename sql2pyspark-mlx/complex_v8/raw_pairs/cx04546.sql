WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix_2,
        CASE WHEN kwh >= 82.9 THEN 'A' WHEN kwh >= 42.1 THEN 'B' ELSE 'C' END AS kwh_tier
    FROM meter_readings
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        quality_prefix,
        kwh_tier,
        ROUND(AVG(kwh) OVER w_site_id, 2) AS site_id_avg_kwh,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id), 2) AS site_id_avg_voltage,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        RANK() OVER w_quality_by_voltage AS voltage_rank
    FROM filtered
    WINDOW w_site_id AS (PARTITION BY site_id), w_quality_by_voltage AS (PARTITION BY quality ORDER BY voltage DESC, id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(voltage - AVG(voltage) OVER w_voltage_rank_site_id, 2) AS voltage_vs_voltage_rank_site_id_avg,
        RANK() OVER (ORDER BY rows_in_site_id) AS rows_in_site_id_rank,
        COUNT(*) OVER (PARTITION BY voltage_rank, site_id) AS rows_in_voltage_rank_site_id,
        MAX(rows_in_site_id) OVER w_voltage_rank_site_id AS voltage_rank_site_id_max_rows_in_site_id,
        RANK() OVER (PARTITION BY quality, site_id ORDER BY site_id_avg_kwh) AS site_id_avg_kwh_rank
    FROM sequenced
    WINDOW w_voltage_rank_site_id AS (PARTITION BY voltage_rank, site_id), w_quality_site_id_by_site_id_a AS (PARTITION BY quality, site_id ORDER BY site_id_avg_kwh)
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh < 104.8
        )
      AND s.site_id_avg_voltage <= 232.8
),
joined AS (
    -- Attach site attributes
    SELECT s.*, si.installed_kw AS site_installed_kw, si.site_name
    FROM qualified s
    JOIN sites si ON si.id = s.site_id
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality_prefix,
        s.rows_in_site_id,
        s.voltage_rank,
        s.voltage_vs_voltage_rank_site_id_avg,
        s.rows_in_voltage_rank_site_id,
        s.voltage_rank_site_id_max_rows_in_site_id,
        s.site_id_avg_kwh_rank,
        s.site_installed_kw,
        s.site_name,
        si.zone AS site_zone
    FROM joined s
    JOIN sites si ON si.id = s.site_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality_prefix,
        s.rows_in_site_id,
        s.voltage_rank,
        s.voltage_vs_voltage_rank_site_id_avg,
        s.rows_in_voltage_rank_site_id,
        s.voltage_rank_site_id_max_rows_in_site_id,
        s.site_id_avg_kwh_rank,
        s.site_installed_kw,
        s.site_zone,
        si.site_name AS site_site_name
    FROM joined_site s
    JOIN sites si ON si.id = s.site_id AND si.commissioned_on < DATE '2025-11-28'
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    quality_prefix,
    rows_in_site_id,
    voltage_rank,
    voltage_rank_site_id_max_rows_in_site_id,
    site_id_avg_kwh_rank,
    site_installed_kw,
    site_site_name,
    CASE WHEN site_zone IN ('Z3', 'Z4') THEN 'group_a' WHEN site_zone = 'Z2' THEN 'group_b' ELSE 'other' END AS site_zone_group,
    DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
FROM with_parent
ORDER BY site_zone_group DESC, id
LIMIT 20
