WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CASE WHEN voltage >= 233.2 THEN 'large' WHEN voltage >= 218.2 THEN 'medium' ELSE 'small' END AS voltage_tier
    FROM meter_readings
    WHERE voltage >= 219.5
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        si.commissioned_on AS site_commissioned_on
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
    WHERE YEAR(si.commissioned_on) = 2025
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        voltage,
        site_commissioned_on,
        ROUND(PERCENT_RANK() OVER w_all_by_site_commissioned_on, 3) AS site_commissioned_on_percent_rank,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY YEAR(site_commissioned_on), site_id), 0), 4) AS voltage_share_of_site_commissioned_on_year_site_,
        SUM(voltage) OVER (PARTITION BY site_id) AS site_id_sum_voltage,
        ROW_NUMBER() OVER w_all_by_site_commissioned_on AS site_commissioned_on_seq,
        RANK() OVER (PARTITION BY site_id ORDER BY voltage DESC NULLS FIRST, id) AS voltage_rank,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY voltage ASC) AS voltage_dense_rank,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY YEAR(site_commissioned_on), site_id), 2) AS voltage_vs_site_commissioned_on_year_site_id_avg,
        NTILE(4) OVER (PARTITION BY site_id ORDER BY site_commissioned_on NULLS LAST, id) AS site_commissioned_on_quartile,
        MAX(voltage) OVER (PARTITION BY site_id ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far,
        LAST_VALUE(site_commissioned_on) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY site_commissioned_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_site_commissioned_on,
        DENSE_RANK() OVER w_site_id_by_site_commissioned_4 AS site_commissioned_on_dense_rank
    FROM joined
    WINDOW w_all_by_site_commissioned_on AS (ORDER BY site_commissioned_on DESC, id), w_site_id_by_site_commissioned_4 AS (PARTITION BY site_id ORDER BY site_commissioned_on NULLS LAST)
)
-- Final projection
SELECT
    id,
    site_id,
    site_commissioned_on,
    site_commissioned_on_percent_rank,
    voltage_share_of_site_commissioned_on_year_site_,
    voltage_dense_rank,
    voltage_vs_site_commissioned_on_year_site_id_avg,
    last_known_site_commissioned_on,
    site_commissioned_on_dense_rank,
    CASE WHEN site_id_sum_voltage >= 1554.0 THEN 'HIGH' WHEN site_id_sum_voltage >= 1106.0 THEN 'MEDIUM' ELSE 'LOW' END AS site_id_sum_voltage_tier,
    CASE WHEN voltage_rank >= 6 THEN 'HIGH' WHEN voltage_rank >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_rank_tier
FROM windowed
ORDER BY site_commissioned_on DESC, voltage_vs_site_commissioned_on_year_site_id_avg DESC, id
LIMIT 20
