WITH in_window AS (
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
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(energy_cost) OVER w_zone AS zone_min_energy_cost,
        MAX(energy_cost) OVER (PARTITION BY site_id, zone ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_energy_cost_so_far,
        DENSE_RANK() OVER (ORDER BY read_at ASC) AS read_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY site_id, zone ORDER BY read_at ASC, id) AS read_at_seq
    FROM in_window
    WINDOW w_zone AS (PARTITION BY zone)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        kwh,
        quality,
        zone,
        energy_cost,
        zone_min_energy_cost,
        max_energy_cost_so_far,
        read_at_dense_rank,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY TO_DATE(read_at), zone)) AS distinct_site_id_in_read_at_to_date_zone,
        COUNT(*) OVER (PARTITION BY site_id, read_at_dense_rank ORDER BY read_at_seq DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        COUNT(*) OVER w_read_at_to_date_zone AS rows_in_read_at_to_date_zone
    FROM windowed
    WINDOW w_read_at_to_date_zone AS (PARTITION BY TO_DATE(read_at), zone)
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(read_at) AS first_reading_read_at, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM sequenced s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    quality,
    zone,
    energy_cost,
    max_energy_cost_so_far,
    read_at_dense_rank,
    distinct_site_id_in_read_at_to_date_zone,
    running_count,
    first_reading_read_at,
    reading_count,
    CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
FROM summarized_rows
WHERE HOUR(first_reading_read_at) BETWEEN 8 AND 13
ORDER BY quality_group
