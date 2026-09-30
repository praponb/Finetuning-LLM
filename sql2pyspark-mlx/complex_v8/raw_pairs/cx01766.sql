WITH joined_ranges AS (
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
derived AS (
    -- Label and score rows
    SELECT *, HOUR(played_at) AS played_at_hour
    FROM joined_ranges
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(prior_rows_12h, 0) AS prior_rows_12h_filled,
        ABS(played_at_hour - 11) AS played_at_hour_distance,
        DAYOFWEEK(played_at) AS played_at_dayofweek
    FROM derived
    WHERE TO_DATE(played_at) < DATE '2025-03-02'
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_rows_12h,
        s.prior_ms_played_12h,
        s.played_at_hour,
        s.prior_rows_12h_filled,
        s.played_at_dayofweek,
        p.id AS play_id,
        p.device AS play_device,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played
    FROM derived_2 s
    JOIN plays p ON p.listener_id = s.listener_id
)
SELECT *
FROM (
    SELECT played_at_hour, play_id, play_device, prior_ms_played_12h
    FROM exploded_detail
) src
PIVOT (
    MIN(prior_ms_played_12h)
    FOR play_device IN ('speaker', 'phone', 'desktop', 'car')
)
