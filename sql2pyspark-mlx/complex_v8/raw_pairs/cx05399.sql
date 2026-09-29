WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage,
        LENGTH(TRIM(quality)) AS quality_len,
        ROUND(kwh * 0.25, 2) AS kwh_adj
    FROM meter_readings
    WHERE kwh <= 83.8
      AND site_id IS NOT NULL
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.greatest_kwh_voltage,
        s.kwh_adj,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh
    FROM base s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh_adj,
        s.reading_id,
        s.reading_quality,
        s.reading_read_at,
        s.reading_kwh,
        si.installed_kw AS site_installed_kw,
        m.read_at AS reading_read_at_2,
        m.kwh AS reading_kwh_2
    FROM with_children_readings s
    JOIN sites si ON si.id = s.site_id
    LEFT JOIN meter_readings m ON m.id = s.reading_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY reading_quality ORDER BY kwh_adj DESC NULLS FIRST) AS kwh_adj_dense_rank,
        NTILE(5) OVER (PARTITION BY reading_quality ORDER BY kwh_adj DESC NULLS FIRST, id, reading_id) AS kwh_adj_quintile,
        ROUND(reading_kwh_2 / NULLIF(SUM(reading_kwh_2) OVER (PARTITION BY reading_quality), 0), 4) AS reading_kwh_2_share_of_reading_quality,
        NTH_VALUE(reading_kwh_2, 2) OVER (PARTITION BY reading_quality ORDER BY kwh_adj DESC NULLS FIRST, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reading_kwh_2,
        LEAD(reading_kwh) OVER (PARTITION BY reading_quality ORDER BY kwh_adj DESC NULLS FIRST, id, reading_id) AS next_reading_kwh,
        RANK() OVER (PARTITION BY reading_quality ORDER BY kwh_adj DESC NULLS FIRST) AS kwh_adj_rank
    FROM with_parent
)
SELECT id, reading_id, metric_name, reading
FROM (
    SELECT id, reading_id, kwh_adj_quintile, kwh_adj_dense_rank
    FROM with_trends
) src
UNPIVOT (
    reading FOR metric_name IN (kwh_adj_quintile AS kwh_adj_quintile_v, kwh_adj_dense_rank AS kwh_adj_dense_rank_v)
) unp
ORDER BY id, reading_id, metric_name
