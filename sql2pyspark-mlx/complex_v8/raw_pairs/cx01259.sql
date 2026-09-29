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
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM joined_ranges s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
      AND s.zone IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        rate,
        LAG(rate) OVER (PARTITION BY quality, site_id ORDER BY kwh, id) AS prev_rate,
        LAST_VALUE(read_at) OVER (PARTITION BY MONTH(read_at) ORDER BY energy_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at
    FROM screened
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    rate,
    last_read_at,
    INITCAP(TRIM(quality)) AS quality_clean
FROM windowed
ORDER BY rate DESC NULLS FIRST, kwh
