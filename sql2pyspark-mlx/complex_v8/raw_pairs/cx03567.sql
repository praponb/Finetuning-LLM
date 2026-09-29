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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_zone_by_read_at AS seq_no,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY zone)) AS distinct_site_id_in_zone
    FROM covered
    WINDOW w_zone_by_read_at AS (PARTITION BY zone ORDER BY read_at, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality), 0), 4) AS kwh_share_of_quality,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at
    FROM sequenced
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    zone,
    rate,
    seq_no,
    distinct_site_id_in_zone,
    kwh_share_of_quality
FROM with_windows
WHERE kwh BETWEEN 34.6 AND 96.8
