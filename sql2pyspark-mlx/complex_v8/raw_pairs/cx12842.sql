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
summary AS (
    -- Group-level totals
    SELECT
        quality,
        TO_DATE(read_at) AS read_at_day,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        MIN(kwh) AS min_kwh,
        ARRAY_SORT(COLLECT_SET(zone)) AS zone_list,
        MAX(read_at) AS last_read_at,
        ROUND(100 * COUNT(CASE WHEN quality <> 'BAD' THEN 1 END) / COUNT(*), 1) AS pct_quality_bad
    FROM in_window
    WHERE kwh >= 24.5
    GROUP BY 1, 2
)
-- Final projection
SELECT
    quality,
    read_at_day,
    distinct_site_ids,
    zone_list,
    pct_quality_bad,
    ADD_MONTHS(read_at_day, 12) AS read_at_day_plus_12m,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
FROM summary
