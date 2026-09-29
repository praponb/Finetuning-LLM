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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(rate, 3) OVER (PARTITION BY zone ORDER BY kwh DESC, id) AS prev3_rate,
        DENSE_RANK() OVER (PARTITION BY zone ORDER BY kwh DESC, id) AS kwh_dense_rank,
        COUNT(*) OVER w_quality_site_id AS rows_in_quality_site_id
    FROM in_window
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id)
)
SELECT
    id,
    site_id,
    kwh,
    quality,
    zone,
    rate,
    energy_cost,
    prev3_rate,
    kwh_dense_rank,
    rows_in_quality_site_id,
    LENGTH(TRIM(quality)) AS quality_len,
    ROUND(rate / NULLIF(kwh_dense_rank, 0), 3) AS rate_per_kwh_dense_rank
FROM metrics
WHERE site_id <= 8
ORDER BY rate DESC, id
LIMIT 20
