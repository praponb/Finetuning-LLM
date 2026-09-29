WITH joined_ranges AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.voltage,
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
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(read_at) AS read_at_day,
        site_id,
        zone,
        COUNT(*) AS cnt,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        COUNT(*) FILTER (WHERE zone NOT IN ('Z3', 'Z4')) AS n_zone_not_in_z3_z4,
        MAX(read_at) AS last_read_at
    FROM joined_ranges
    WHERE kwh < 104.8
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 3
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.read_at_day,
        s.site_id,
        s.zone,
        s.distinct_site_ids,
        s.n_zone_not_in_z3_z4,
        s.last_read_at,
        si.tariff AS site_tariff
    FROM grouped s
    JOIN sites si ON si.id = s.site_id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(distinct_site_ids / 2) * 2 AS distinct_site_ids_bucket,
        SUBSTRING(TRIM(zone), 1, 2) AS zone_prefix
    FROM with_details
)
SELECT read_at_day, site_id, measure, amount
FROM (
    SELECT read_at_day, site_id, distinct_site_ids_bucket, n_zone_not_in_z3_z4, distinct_site_ids
    FROM scored
) src
UNPIVOT INCLUDE NULLS (
    amount FOR measure IN (distinct_site_ids_bucket, n_zone_not_in_z3_z4, distinct_site_ids)
) unp
