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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        zone,
        site_id,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        ROUND(AVG(rate), 2) AS avg_rate,
        ARRAY_SORT(COLLECT_SET(zone)) AS zone_list,
        COUNT(DISTINCT zone) AS distinct_zones,
        MIN(read_at) AS first_read_at,
        COUNT(*) AS n_rows,
        ROUND(SUM(energy_cost) / NULLIF(SUM(rate), 0), 3) AS energy_cost_to_rate_ratio
    FROM joined_ranges
    GROUP BY zone, site_id, DATE_TRUNC('MONTH', read_at)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN n_rows >= 7 THEN 'large' WHEN n_rows >= 4 THEN 'medium' ELSE 'small' END AS n_rows_tier
    FROM per_group
    WHERE energy_cost_to_rate_ratio < 7.5
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        read_at_month + INTERVAL 7 DAYS AS read_at_month_plus_7d,
        DATEDIFF(DATE '2025-12-31', read_at_month) AS days_since_read_at_month,
        LEAST(n_rows, avg_rate) AS least_n_rows_avg_rate
    FROM labeled
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT n_rows, read_at_month, zone, avg_rate
    FROM enriched
) src
PIVOT (
    MIN(avg_rate)
    FOR zone IN ('Z2', 'Z3')
)
