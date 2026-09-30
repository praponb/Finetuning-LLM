WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        plan,
        country,
        birth_year,
        interests,
        DATE_FORMAT(signup_date, 'yyyy-MM-dd') AS signup_date_label,
        DAYOFWEEK(signup_date) AS signup_date_dayofweek,
        ABS(birth_year - 1970) AS birth_year_distance
    FROM listeners
    WHERE birth_year < 1981
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.plan,
        s.country,
        s.birth_year,
        s.signup_date_dayofweek,
        s.birth_year_distance,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source,
        p.track_id AS play_track_id,
        p.is_skipped AS play_is_skipped
    FROM recent s
    JOIN plays p ON p.listener_id = s.id
),
with_details_play AS (
    -- Attach play attributes
    SELECT
        s.id,
        s.plan,
        s.birth_year,
        s.signup_date_dayofweek,
        s.birth_year_distance,
        s.play_id,
        s.play_source,
        s.play_track_id,
        s.play_is_skipped,
        p.played_at AS play_played_at,
        p.source AS play_source_2
    FROM exploded_detail_plays s
    JOIN plays p ON p.id = s.play_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        signup_date_dayofweek,
        play_id,
        play_source,
        play_track_id,
        play_played_at,
        LAG(birth_year_distance, 3, 0) OVER (ORDER BY birth_year DESC, id, play_id) AS prev3_birth_year_distance,
        ROUND(signup_date_dayofweek / NULLIF(SUM(signup_date_dayofweek) OVER (PARTITION BY play_is_skipped), 0), 4) AS signup_date_dayofweek_share_of_play_is_skipped
    FROM with_details_play
)
SELECT *
FROM (
    SELECT play_played_at, play_id, play_source, prev3_birth_year_distance
    FROM with_windows
) src
PIVOT (
    SUM(prev3_birth_year_distance) AS amount, MAX(prev3_birth_year_distance) AS max_v
    FOR play_source IN ('search', 'album', 'playlist', 'radio')
)
ORDER BY play_played_at, play_id
