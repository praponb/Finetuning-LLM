WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.ms_played) AS prior_ms_played_12h
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.listener_id, a.played_at
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.ms_played <= 302279
              AND p.source <> 'radio'
        )
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        played_at,
        prior_rows_12h,
        CASE WHEN prior_ms_played_12h >= 1600000 THEN 'HIGH' WHEN prior_ms_played_12h >= 700000 THEN 'MEDIUM' ELSE 'LOW' END AS prior_ms_played_12h_tier,
        YEAR(played_at) AS played_at_year,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label
    FROM with_activity
),
unique_rows AS (
    -- Keep the earliest row per prior_ms_played_12h_tier
    SELECT
        id,
        played_at,
        prior_rows_12h,
        prior_ms_played_12h_tier,
        played_at_year,
        played_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY prior_ms_played_12h_tier ORDER BY played_at, id) AS recency_rank
        FROM derived
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, played_at_year, prior_rows_12h
    FROM unique_rows
) src
UNPIVOT (
    metric_value FOR metric_name IN (played_at_year AS played_at_year_v, prior_rows_12h AS prior_rows_12h_v)
) unp
ORDER BY id, metric_name
