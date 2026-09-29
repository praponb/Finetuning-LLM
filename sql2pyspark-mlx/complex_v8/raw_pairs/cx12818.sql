WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        read_at + INTERVAL 30 MINUTES AS read_at_plus_30m
    FROM meter_readings
    WHERE kwh BETWEEN 37.1 AND 90.6
      AND MONTH(read_at) IN (1, 3)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        read_at_plus_30m,
        NTH_VALUE(read_at_plus_30m, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_plus_30m,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY TO_DATE(read_at_plus_30m), site_id), 0), 4) AS kwh_share_of_read_at_plus_30m_to_date_site_id,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at
    FROM cleaned_readings
),
enriched_site AS (
    -- Join sites
    SELECT s.*, si.site_name
    FROM metrics s
    JOIN sites si ON si.id = s.site_id
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM enriched_site s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'OK'
        )
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        read_at_plus_30m,
        second_read_at_plus_30m,
        last_known_read_at,
        site_name,
        NTILE(4) OVER (PARTITION BY site_id ORDER BY voltage, id) AS voltage_quartile,
        ROUND(voltage - AVG(voltage) OVER w_site_id, 2) AS voltage_vs_site_id_avg,
        DENSE_RANK() OVER w_site_id_by_voltage_ties AS voltage_dense_rank
    FROM screened
    WINDOW w_site_id AS (PARTITION BY site_id), w_site_id_by_voltage_ties AS (PARTITION BY site_id ORDER BY voltage)
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    read_at_plus_30m,
    last_known_read_at,
    site_name,
    voltage_quartile,
    second_read_at_plus_30m + INTERVAL 30 MINUTES AS second_read_at_plus_30m_plus_30m
FROM with_trends
ORDER BY id DESC
