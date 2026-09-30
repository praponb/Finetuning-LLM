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
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        quality,
        zone,
        rate,
        ROUND(AVG(rate) OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY read_at ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS rate_avg_2,
        SUM(kwh) OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY read_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS kwh_sum_7
    FROM in_window
),
deduped AS (
    -- Keep the latest row per zone, quality
    SELECT
        id,
        site_id,
        read_at,
        quality,
        zone,
        rate,
        rate_avg_2,
        kwh_sum_7
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone, quality ORDER BY kwh_sum_7 DESC, id DESC) AS row_num
        FROM windowed
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    site_id,
    rate,
    rate_avg_2,
    kwh_sum_7,
    TO_DATE(read_at) AS read_at_date,
    TO_DATE(read_at) AS read_at_date_2
FROM deduped
ORDER BY read_at_date_2 DESC, id
LIMIT 10
