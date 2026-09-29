WITH joined_ranges AS (
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
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        QUARTER(read_at) AS read_at_quarter,
        ABS(prior_rows_7d - 3) AS prior_rows_7d_distance
    FROM joined_ranges
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label,
        CASE WHEN read_at_quarter >= 3 THEN 'large' WHEN read_at_quarter >= 2 THEN 'medium' ELSE 'small' END AS read_at_quarter_tier
    FROM tagged
    WHERE site_id IS NOT NULL
)
SELECT
    id,
    site_id,
    read_at,
    prior_rows_7d,
    read_at_quarter,
    prior_rows_7d_distance,
    read_at_label,
    CONCAT_WS(': ', read_at_label, read_at_quarter_tier) AS read_at_label_read_at_quarter_tier_label
FROM annotated
ORDER BY read_at_label NULLS LAST
