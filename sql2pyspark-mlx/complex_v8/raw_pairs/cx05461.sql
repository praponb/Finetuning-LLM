WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE artist_id IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        moods,
        duration_sec_filled,
        LAG(duration_sec_filled, 3) OVER w_is_explicit_artist_id_by_dur AS prev3_duration_sec_filled,
        LEAD(duration_sec, 2) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec_filled DESC, id) AS next2_duration_sec,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id) AS rows_in_is_explicit_artist_id,
        DENSE_RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec_filled DESC) AS duration_sec_filled_dense_rank
    FROM scoped_tracks
    WINDOW w_is_explicit_artist_id_by_dur AS (PARTITION BY is_explicit, artist_id ORDER BY duration_sec_filled DESC, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        COALESCE(next2_duration_sec, 0) AS next2_duration_sec_filled
    FROM with_windows
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.moods,
        s.prev3_duration_sec_filled,
        s.next2_duration_sec,
        s.rows_in_is_explicit_artist_id,
        s.title_slug,
        a.genre AS artist_genre
    FROM derived s
    JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        moods,
        next2_duration_sec,
        rows_in_is_explicit_artist_id,
        artist_genre,
        LAST_VALUE(prev3_duration_sec_filled) OVER (PARTITION BY artist_genre, is_explicit ORDER BY next2_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prev3_duration_sec_filled,
        LAST_VALUE(prev3_duration_sec_filled) IGNORE NULLS OVER (PARTITION BY artist_genre, is_explicit ORDER BY next2_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev3_duration_sec_filled,
        ROUND(AVG(prev3_duration_sec_filled) OVER (PARTITION BY is_explicit, artist_id), 2) AS is_explicit_artist_id_avg_prev3_duration_sec_fil,
        ROUND(prev3_duration_sec_filled - AVG(prev3_duration_sec_filled) OVER w_is_explicit_artist_id, 2) AS prev3_duration_sec_filled_vs_is_explicit_artist_
    FROM with_parent_artist
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
),
derived_2 AS (
    -- Label and score rows
    SELECT *, ABS(next2_duration_sec - 352) AS next2_duration_sec_distance
    FROM metrics
    WHERE rows_in_is_explicit_artist_id BETWEEN 4 AND 15
)
-- Final projection
SELECT
    id,
    artist_id,
    moods,
    next2_duration_sec,
    rows_in_is_explicit_artist_id,
    is_explicit_artist_id_avg_prev3_duration_sec_fil,
    next2_duration_sec_distance
FROM derived_2
ORDER BY artist_id, id
LIMIT 20
