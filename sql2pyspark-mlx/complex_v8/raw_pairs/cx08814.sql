WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods
    FROM tracks
    WHERE bpm > 96
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        bpm - duration_sec AS bpm_minus_duration_sec,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM filtered_tracks
    WHERE bpm >= 82
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.moods,
        s.bpm_minus_duration_sec,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.listener_id AS play_listener_id
    FROM derived s
    JOIN plays p ON p.track_id = s.id
),
artist_id_stats AS (
    -- Max bpm_minus_duration_sec per artist_id
    SELECT artist_id, MAX(bpm_minus_duration_sec) AS max_bpm_minus_duration_sec
    FROM line_level_plays
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_bpm_minus_duration_sec,
        ROUND(s.bpm_minus_duration_sec - g.max_bpm_minus_duration_sec, 2) AS bpm_minus_duration_sec_minus_max
    FROM line_level_plays s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm_minus_duration_sec = g.max_bpm_minus_duration_sec
),
joined AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.moods,
        s.bpm_minus_duration_sec,
        s.play_id,
        s.play_played_at,
        s.play_listener_id,
        s.max_bpm_minus_duration_sec,
        s.bpm_minus_duration_sec_minus_max,
        l.username AS listener_username,
        l.birth_year AS listener_birth_year
    FROM compared s
    JOIN listeners l ON l.id = s.play_listener_id
)
SELECT
    id,
    artist_id,
    duration_sec,
    moods,
    bpm_minus_duration_sec,
    play_id,
    play_played_at,
    play_listener_id,
    max_bpm_minus_duration_sec,
    bpm_minus_duration_sec_minus_max,
    listener_username,
    listener_birth_year,
    FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
    LOWER(REGEXP_REPLACE(TRIM(listener_username), ' ', '_')) AS listener_username_slug
FROM joined
ORDER BY artist_id, bpm_minus_duration_sec
