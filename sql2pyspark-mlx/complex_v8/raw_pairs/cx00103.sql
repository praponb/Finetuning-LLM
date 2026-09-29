WITH covered AS (
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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN m.quality NOT IN ('EST', 'OK') THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT MAX(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_kwh,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM covered s
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, TO_DATE(read_at) ORDER BY reading_quality_count DESC NULLS FIRST), 3) AS reading_quality_count_percent_rank,
        ROUND(energy_cost / NULLIF(SUM(energy_cost) OVER (PARTITION BY zone), 0), 4) AS energy_cost_share_of_zone
    FROM with_stats
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        zone,
        energy_cost,
        last_reading_read_at,
        reading_quality_count_percent_rank,
        energy_cost_share_of_zone,
        FLOOR(energy_cost / 250) * 250 AS energy_cost_bucket
    FROM sequenced
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    quality,
    zone,
    energy_cost,
    last_reading_read_at,
    reading_quality_count_percent_rank
FROM derived
ORDER BY last_reading_read_at NULLS LAST, kwh NULLS LAST
