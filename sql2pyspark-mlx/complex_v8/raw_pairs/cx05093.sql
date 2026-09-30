WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE quality IS NOT NULL
      AND read_at >= TIMESTAMP '2025-02-06 08:00:00'
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_date,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM filtered s
    JOIN meter_readings m ON m.site_id = s.site_id AND MONTH(m.read_at) IN (2, 3)
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM with_children_readings
    WHERE voltage > (SELECT MIN(voltage) FROM with_children_readings)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(voltage) OVER (PARTITION BY reading_quality, site_id ORDER BY read_at, id, reading_id) AS next_voltage,
        ROUND(AVG(reading_voltage) OVER (PARTITION BY YEAR(read_at_date) ORDER BY reading_read_at DESC, id, reading_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS reading_voltage_avg_3,
        DENSE_RANK() OVER (PARTITION BY YEAR(read_at_date) ORDER BY reading_read_at DESC, id, reading_id) AS reading_read_at_dense_rank,
        voltage - LAG(voltage) OVER w_reading_quality_site_id_by_r AS voltage_change,
        DENSE_RANK() OVER (PARTITION BY reading_quality, site_id ORDER BY read_at, id, reading_id) AS read_at_dense_rank
    FROM without_matches
    WINDOW w_reading_quality_site_id_by_r AS (PARTITION BY reading_quality, site_id ORDER BY read_at, id, reading_id)
),
combined_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_date,
        s.reading_id,
        s.reading_quality,
        s.next_voltage,
        s.reading_voltage_avg_3,
        s.voltage_change,
        s.read_at_dense_rank,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM sequenced s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone IN ('Z4', 'Z3')
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        quality,
        read_at_date,
        reading_id,
        reading_quality,
        next_voltage,
        read_at_dense_rank,
        site_tariff,
        FIRST_VALUE(voltage_change) IGNORE NULLS OVER (PARTITION BY quality ORDER BY read_at_dense_rank ASC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage_change,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY read_at_dense_rank ASC) AS read_at_dense_rank_dense_rank,
        DENSE_RANK() OVER w_quality_by_read_at_dense_ran_3 AS read_at_dense_rank_dense_rank_2,
        COUNT(*) OVER (PARTITION BY site_tariff) AS rows_in_site_tariff,
        RANK() OVER w_quality_by_read_at_dense_ran_3 AS read_at_dense_rank_rank,
        SIZE(COLLECT_SET(site_zone) OVER (PARTITION BY site_tariff)) AS distinct_site_zone_in_site_tariff
    FROM combined_details
    WINDOW w_quality_by_read_at_dense_ran_3 AS (PARTITION BY quality ORDER BY read_at_dense_rank ASC, id, reading_id)
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.read_at,
        s.quality,
        s.read_at_date,
        s.reading_id,
        s.next_voltage,
        s.site_tariff,
        s.first_known_voltage_change,
        s.read_at_dense_rank_dense_rank,
        s.read_at_dense_rank_dense_rank_2,
        s.rows_in_site_tariff,
        s.read_at_dense_rank_rank,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM sequenced_2 s
    JOIN meter_readings m ON m.id = s.reading_id
    WHERE m.kwh <= 109.0
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, reading_id, metric_name, value
FROM (
    SELECT id, reading_id, rows_in_site_tariff, read_at_dense_rank_dense_rank
    FROM joined
) src
UNPIVOT (
    value FOR metric_name IN (rows_in_site_tariff, read_at_dense_rank_dense_rank)
) unp
