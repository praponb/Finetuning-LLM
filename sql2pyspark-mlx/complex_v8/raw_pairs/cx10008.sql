WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE kwh < 90.6
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality_ref,
        ROW_NUMBER() OVER (PARTITION BY YEAR(read_at), site_id ORDER BY read_at DESC, id) AS rn,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id), 0), 4) AS voltage_share_of_site_id,
        kwh - LAG(kwh) OVER (PARTITION BY YEAR(read_at), site_id ORDER BY read_at DESC, id) AS kwh_change,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id), 2) AS kwh_vs_site_id_avg,
        SUM(voltage) OVER (PARTITION BY YEAR(read_at), site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY YEAR(read_at), site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage
    FROM valid
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality_ref,
        voltage_share_of_site_id,
        kwh_change,
        kwh_vs_site_id_avg,
        running_voltage,
        second_voltage
    FROM ranked
    WHERE rn <= 3
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality_ref,
        voltage_share_of_site_id,
        kwh_change,
        kwh_vs_site_id_avg,
        running_voltage,
        second_voltage,
        NTH_VALUE(second_voltage, 2) OVER (PARTITION BY YEAR(read_at) ORDER BY kwh_change ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_voltage,
        ROUND(kwh_change / NULLIF(SUM(kwh_change) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)), 0), 4) AS kwh_change_share_of_read_at_month,
        RANK() OVER (PARTITION BY YEAR(read_at) ORDER BY kwh_change ASC, id) AS kwh_change_rank,
        NTH_VALUE(running_voltage, 2) OVER (PARTITION BY TO_DATE(read_at) ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_voltage
    FROM top_per_group
    WINDOW w_read_at_year_by_kwh_change AS (PARTITION BY YEAR(read_at) ORDER BY kwh_change ASC, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, kwh_vs_site_id_avg, second_running_voltage
    FROM sequenced
) src
UNPIVOT (
    value FOR metric IN (kwh_vs_site_id_avg AS m_kwh_vs_site_id_avg, second_running_voltage AS second_running_voltage_v)
) unp
