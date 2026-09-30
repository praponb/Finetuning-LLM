WITH cleaned AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        signup_date,
        interests,
        DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_date_label,
        DATEDIFF(DATE '2025-12-31', signup_date) AS days_since_signup_date
    FROM listeners
    WHERE country IS NOT NULL
      AND LENGTH(TRIM(username)) >= 8
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.username,
        s.signup_date,
        s.interests,
        s.signup_date_label,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.device AS play_device,
        p.track_id AS play_track_id,
        p.is_skipped AS play_is_skipped
    FROM cleaned s
    JOIN plays p ON p.listener_id = s.id AND p.track_id IS NOT NULL
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', signup_date) AS signup_date_month,
        play_track_id,
        COUNT(*) AS row_count,
        ROUND(AVG(play_ms_played), 2) AS avg_play_ms_played,
        SUM(CASE WHEN play_device = 'speaker' THEN play_ms_played ELSE 0 END) AS play_ms_played_play_device_speaker,
        COUNT(DISTINCT play_track_id) AS distinct_play_track_ids,
        MIN(signup_date) AS first_signup_date,
        SUM(play_ms_played) FILTER (WHERE LENGTH(TRIM(username)) >= 7) AS play_ms_played_length_trim_username_7
    FROM with_children_plays
    GROUP BY DATE_TRUNC('MONTH', signup_date), play_track_id
    HAVING MAX(play_ms_played) > 52009
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM totals s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.play_track_id
              AND p.ms_played >= 72464
        )
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE avg_play_ms_played > (SELECT AVG(avg_play_ms_played) FROM qualified)
)
-- Final projection
SELECT
    signup_date_month,
    play_track_id,
    row_count,
    avg_play_ms_played,
    play_ms_played_play_device_speaker,
    distinct_play_track_ids,
    first_signup_date
FROM screened
ORDER BY signup_date_month, play_track_id
LIMIT 5
