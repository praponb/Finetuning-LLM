WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE artist_id IS NOT NULL
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        UPPER(TRIM(title)) AS title_clean,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM scoped
    WHERE artist_id <= 11
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        title_prefix,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id)) AS days_since_prev_release_date,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date))) AS distinct_artist_id_in_release_date_month,
        LAG(release_date) OVER (ORDER BY release_date NULLS LAST, id) AS prev_release_date,
        ROW_NUMBER() OVER (ORDER BY release_date NULLS LAST, id) AS seq_no,
        LAG(release_date) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id) AS prev_release_date_2
    FROM scored
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.title_prefix,
        s.days_since_prev_release_date,
        s.distinct_artist_id_in_release_date_month,
        s.prev_release_date,
        s.seq_no,
        s.prev_release_date_2,
        t.mood_pos,
        t.mood
    FROM windowed s
    LATERAL VIEW OUTER POSEXPLODE(s.moods) t AS mood_pos, mood
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN mood IN ('sad', 'chill') THEN 'group_a' WHEN mood = 'workout' THEN 'group_b' ELSE 'other' END AS mood_group,
        CONCAT_WS(': ', title, title_prefix) AS title_title_prefix_label
    FROM per_label
    WHERE prev_release_date_2 < DATE '2024-12-14'
),
joined AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.title_prefix,
        s.days_since_prev_release_date,
        s.distinct_artist_id_in_release_date_month,
        s.prev_release_date,
        s.mood_pos,
        s.mood,
        s.title_title_prefix_label,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre
    FROM tagged s
    LEFT JOIN artists a ON a.id = s.artist_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.track_id = s.id) AS max_play_ms_played
    FROM joined s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, mood_pos, field, value
FROM (
    SELECT id, mood_pos, distinct_artist_id_in_release_date_month, days_since_prev_release_date
    FROM summarized_rows
) src
UNPIVOT (
    value FOR field IN (distinct_artist_id_in_release_date_month, days_since_prev_release_date)
) unp
