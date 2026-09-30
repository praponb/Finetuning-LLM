WITH joined_ranges AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
unique_rows AS (
    -- Keep the earliest row per quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        zone,
        rate,
        energy_cost
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY kwh, id) AS row_num
        FROM joined_ranges
    ) ranked
    WHERE row_num = 1
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT site_id, MIN(rate) AS min_rate, COUNT(*) AS row_count
    FROM unique_rows
    GROUP BY site_id
    HAVING COUNT(*) >= 3
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.site_id,
        s.min_rate,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM totals s
    JOIN meter_readings m ON m.site_id = s.site_id
)
-- Subtotals with ROLLUP
SELECT
    reading_id,
    reading_quality,
    MIN(reading_read_at) AS first_reading_read_at,
    SUM(reading_kwh) FILTER (WHERE reading_quality NOT IN ('EST', 'BAD')) AS reading_kwh_reading_quality_not_in_est_b,
    COUNT(*) FILTER (WHERE reading_quality = 'BAD') AS n_reading_quality_bad
FROM exploded_detail
GROUP BY ROLLUP(reading_id, reading_quality)
ORDER BY reading_id NULLS LAST, reading_quality NULLS LAST
