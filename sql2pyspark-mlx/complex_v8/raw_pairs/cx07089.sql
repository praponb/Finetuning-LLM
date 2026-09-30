WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.kwh) AS prior_kwh_12h
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.site_id, a.read_at
),
site_id_stats AS (
    -- Max prior_rows_12h per site_id
    SELECT site_id, MAX(prior_rows_12h) AS max_prior_rows_12h
    FROM covered
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prior_rows_12h,
        ROUND(s.prior_rows_12h - g.max_prior_rows_12h, 2) AS prior_rows_12h_minus_max
    FROM covered s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.prior_rows_12h = g.max_prior_rows_12h
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label,
        ROUND(prior_rows_12h / NULLIF(prior_kwh_12h, 0), 3) AS prior_rows_12h_per_prior_kwh_12h
    FROM compared
)
SELECT
    id,
    site_id,
    read_at,
    prior_kwh_12h,
    max_prior_rows_12h,
    prior_rows_12h_minus_max,
    read_at_label,
    COALESCE(prior_kwh_12h, 0.0) AS prior_kwh_12h_filled
FROM labeled
ORDER BY id, site_id DESC
LIMIT 5
