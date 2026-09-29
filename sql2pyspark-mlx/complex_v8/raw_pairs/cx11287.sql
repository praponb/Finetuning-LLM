WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 0.9, 2) AS kwh_adj
    FROM meter_readings
    WHERE voltage <= 234.7
      AND quality <> 'EST'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh_adj,
        ROUND(PERCENT_RANK() OVER w_all_by_kwh_ties, 3) AS kwh_percent_rank,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        MAX(kwh) OVER (PARTITION BY site_id) AS site_id_max_kwh
    FROM recent_readings
    WINDOW w_all_by_kwh_ties AS (ORDER BY kwh)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('YEAR', read_at) AS read_at_year,
        ROUND(kwh_percent_rank * 0.9, 2) AS kwh_percent_rank_adj
    FROM with_windows
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.tariff AS site_tariff,
        si.site_name,
        si.installed_kw AS site_installed_kw
    FROM tagged s
    LEFT JOIN sites si ON si.id = s.site_id AND YEAR(si.commissioned_on) = 2025
),
annotated AS (
    -- Add calculated columns
    SELECT *, FLOOR(site_id_max_kwh / 250) * 250 AS site_id_max_kwh_bucket
    FROM enriched
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM annotated s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
        )
)
SELECT
    id,
    site_id,
    read_at,
    kwh_adj,
    rows_in_site_id,
    site_id_max_kwh,
    kwh_percent_rank_adj,
    site_tariff,
    site_name,
    site_installed_kw
FROM filtered
