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
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(read_at) AS read_at_day,
        site_id,
        zone,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        MIN(energy_cost) AS min_energy_cost,
        COUNT(*) AS row_count,
        COUNT(DISTINCT site_id) AS distinct_site_ids
    FROM joined_ranges
    WHERE kwh <= 81.1
    GROUP BY TO_DATE(read_at), site_id, zone
    HAVING AVG(rate) > 3.7
),
tagged AS (
    -- Add calculated columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug
    FROM rollup_base
    WHERE min_energy_cost < 675.0
)
-- Final projection
SELECT
    read_at_day,
    site_id,
    zone,
    min_energy_cost,
    row_count,
    distinct_site_ids,
    GREATEST(distinct_site_ids, min_energy_cost) AS greatest_distinct_site_ids_min_energy_cost,
    DATE_FORMAT(read_at_day, 'yyyy-MM-dd') AS read_at_day_label
FROM tagged
ORDER BY distinct_site_ids DESC NULLS FIRST, read_at_day_label NULLS LAST
