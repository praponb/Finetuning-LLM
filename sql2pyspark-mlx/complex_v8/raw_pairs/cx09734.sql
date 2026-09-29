WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.kwh) AS prior_kwh_7d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
enriched_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.prior_rows_7d,
        s.prior_kwh_7d,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM in_window s
    LEFT JOIN sites si ON si.id = s.site_id
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        prior_rows_7d,
        prior_kwh_7d,
        site_name,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM enriched_site
),
labeled_2 AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        prior_rows_7d,
        prior_kwh_7d,
        site_name,
        read_at_dayofweek,
        SUBSTRING(TRIM(site_name), 1, 3) AS site_name_prefix,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM labeled
)
-- Subtotals with GROUPING SETS
SELECT
    prior_rows_7d,
    site_id,
    read_at_dayofweek,
    GROUPING_ID() AS grouping_level,
    SUM(CASE WHEN UPPER(TRIM(site_name)) LIKE 'R%' THEN prior_rows_7d ELSE 0 END) AS prior_rows_7d_upper_trim_site_name_like_r
FROM labeled_2
GROUP BY GROUPING SETS ((prior_rows_7d, site_id, read_at_dayofweek), (prior_rows_7d), (site_id), ())
ORDER BY prior_rows_7d NULLS LAST, site_id NULLS LAST, read_at_dayofweek NULLS LAST
