WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.kwh) AS prior_kwh_1d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
latest_per_key AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        prior_rows_1d,
        prior_kwh_1d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY prior_rows_1d, id) AS recency_rank
        FROM matched
    ) ranked
    WHERE recency_rank = 1
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ABS(prior_kwh_1d - 270.0) AS prior_kwh_1d_distance,
        prior_rows_1d - prior_kwh_1d AS prior_rows_1d_minus_prior_kwh_1d
    FROM latest_per_key
    WHERE prior_kwh_1d > 240.0
)
-- Final projection
SELECT
    id,
    read_at,
    prior_rows_1d,
    prior_kwh_1d,
    ABS(prior_rows_1d - 3) AS prior_rows_1d_distance
FROM enriched
WHERE prior_rows_1d_minus_prior_kwh_1d > 182.4
ORDER BY read_at NULLS LAST, prior_rows_1d_distance DESC, id
LIMIT 10
