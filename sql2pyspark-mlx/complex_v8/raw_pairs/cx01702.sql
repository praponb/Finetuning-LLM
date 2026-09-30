WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh
    FROM cleaned s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        reading_id,
        reading_read_at,
        reading_kwh,
        LAG(reading_read_at, 2) OVER (ORDER BY read_at, id, reading_id) AS prev2_reading_read_at,
        LEAD(kwh, 3, 0) OVER (ORDER BY read_at, id, reading_id) AS next3_kwh,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, TO_DATE(read_at)), 2) AS site_id_read_at_to_date_avg_kwh,
        ROW_NUMBER() OVER (ORDER BY read_at, id, reading_id) AS rn
    FROM exploded_detail_readings
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        quality,
        reading_id,
        reading_read_at,
        prev2_reading_read_at,
        next3_kwh,
        rn,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM with_trends
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(rn) OVER w_site_id_rn, 2) AS site_id_rn_avg_rn,
        DENSE_RANK() OVER w_quality_by_prev2_reading_rea AS prev2_reading_read_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(reading_read_at) - UNIX_TIMESTAMP(LAG(reading_read_at) OVER w_quality_by_prev2_reading_rea_2)) / 60, 1) AS minutes_since_prev_reading_read_at,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_quality_by_prev2_reading_rea_2)) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(PERCENT_RANK() OVER w_quality_by_prev2_reading_rea, 3) AS prev2_reading_read_at_percent_rank,
        ROUND(rn / NULLIF(SUM(rn) OVER (PARTITION BY site_id, rn), 0), 4) AS rn_share_of_site_id_rn
    FROM scored
    WINDOW w_site_id_rn AS (PARTITION BY site_id, rn), w_quality_by_prev2_reading_rea AS (PARTITION BY quality ORDER BY prev2_reading_read_at), w_quality_by_prev2_reading_rea_2 AS (PARTITION BY quality ORDER BY prev2_reading_read_at, id, reading_id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY prev2_reading_read_at_dense_rank ORDER BY minutes_since_prev_read_at, id, reading_id))) / 60, 1) AS minutes_since_prev_read_at_2,
        ROUND((UNIX_TIMESTAMP(reading_read_at) - UNIX_TIMESTAMP(LAG(reading_read_at) OVER (PARTITION BY prev2_reading_read_at_dense_rank ORDER BY minutes_since_prev_read_at, id, reading_id))) / 60, 1) AS minutes_since_prev_reading_read_at_2
    FROM metrics
)
SELECT
    id,
    site_id,
    quality,
    reading_id,
    reading_read_at,
    prev2_reading_read_at,
    next3_kwh,
    rn,
    quality_slug,
    site_id_rn_avg_rn,
    prev2_reading_read_at_dense_rank,
    minutes_since_prev_reading_read_at,
    minutes_since_prev_read_at,
    prev2_reading_read_at_percent_rank,
    minutes_since_prev_read_at_2
FROM sequenced
WHERE quality_slug IS NOT NULL
ORDER BY next3_kwh, id, reading_id
LIMIT 5
