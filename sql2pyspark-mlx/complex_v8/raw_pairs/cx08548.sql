WITH cleaned_listeners AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        DATE_TRUNC('MONTH', signup_date) AS signup_date_month
    FROM listeners
    WHERE UPPER(TRIM(username)) LIKE 'F%'
      AND plan NOT IN ('family', 'student')
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.username,
        s.country,
        s.signup_date,
        s.signup_date_month,
        p.id AS play_id,
        p.device AS play_device,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played
    FROM cleaned_listeners s
    LEFT JOIN plays p ON p.listener_id = s.id AND p.device = 'desktop'
),
enriched_play AS (
    -- Join plays
    SELECT
        s.id,
        s.username,
        s.country,
        s.signup_date,
        s.play_id,
        s.play_played_at,
        p.track_id AS play_track_id,
        p.source AS play_source
    FROM exploded_detail s
    JOIN plays p ON p.id = s.play_id
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.country,
        s.signup_date,
        s.play_id,
        s.play_track_id,
        s.play_source,
        p.id AS play_id_2,
        p.source AS play_source_2,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device
    FROM enriched_play s
    JOIN plays p ON p.listener_id = s.id
)
-- Subtotals with ROLLUP
SELECT
    play_id,
    play_is_skipped,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT play_id_2) AS distinct_play_id_2s
FROM exploded_detail_plays
GROUP BY ROLLUP(play_id, play_is_skipped)
