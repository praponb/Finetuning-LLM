WITH valid AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        is_verified,
        ABS(debut_year - 2006) AS debut_year_distance,
        COALESCE(debut_year, 0) AS debut_year_filled
    FROM artists
    WHERE country = 'TH'
      AND NOT is_verified
),
with_children_tracks AS (
    -- One row per track
    SELECT
        s.id,
        s.country,
        s.debut_year,
        s.is_verified,
        t.id AS track_id,
        t.is_explicit AS track_is_explicit,
        t.moods AS track_moods,
        t.release_date AS track_release_date
    FROM valid s
    LEFT JOIN tracks t ON t.artist_id = s.id
),
flattened AS (
    -- One row per track_mood
    SELECT
        s.id,
        s.country,
        s.debut_year,
        s.is_verified,
        s.track_id,
        s.track_is_explicit,
        s.track_release_date,
        t.track_mood
    FROM with_children_tracks s
    LATERAL VIEW EXPLODE(s.track_moods) t AS track_mood
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ABS(debut_year - 2015) AS debut_year_distance,
        CASE WHEN country IN ('BR', 'JP') THEN 'group_a' WHEN country = 'TH' THEN 'group_b' ELSE 'other' END AS country_group,
        LOWER(REGEXP_REPLACE(TRIM(track_mood), ' ', '_')) AS track_mood_slug
    FROM flattened
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(debut_year_distance) OVER w_track_id AS track_id_min_debut_year_distance,
        DENSE_RANK() OVER (PARTITION BY track_mood, track_id ORDER BY debut_year_distance, id, track_id, track_mood) AS debut_year_distance_dense_rank,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        LEAD(track_release_date) OVER (PARTITION BY track_mood, track_id ORDER BY debut_year_distance, id, track_id, track_mood) AS next_track_release_date
    FROM labeled
    WINDOW w_track_id AS (PARTITION BY track_id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        country,
        debut_year,
        track_id,
        track_is_explicit,
        track_release_date,
        track_mood,
        debut_year_distance,
        track_mood_slug,
        debut_year_distance_dense_rank,
        rows_in_track_id,
        next_track_release_date,
        NTH_VALUE(debut_year, 2) OVER (ORDER BY debut_year_distance DESC NULLS FIRST, id, track_id, track_mood ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year,
        ROUND(PERCENT_RANK() OVER (ORDER BY debut_year_distance DESC NULLS FIRST, id, track_id, track_mood), 3) AS debut_year_distance_percent_rank,
        LAG(debut_year, 2) OVER (PARTITION BY country, debut_year_distance_dense_rank ORDER BY track_id_min_debut_year_distance, id, track_id, track_mood) AS prev2_debut_year,
        MAX(debut_year) OVER (PARTITION BY country, debut_year_distance_dense_rank ORDER BY track_id_min_debut_year_distance, id, track_id, track_mood ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS debut_year_max_2,
        MIN(rows_in_track_id) OVER w_track_is_explicit AS track_is_explicit_min_rows_in_track_id
    FROM with_windows
    WINDOW w_track_is_explicit AS (PARTITION BY track_is_explicit)
)
-- Final projection
SELECT
    id,
    track_id,
    track_release_date,
    debut_year_distance,
    track_mood_slug,
    debut_year_distance_dense_rank,
    rows_in_track_id,
    next_track_release_date,
    second_debut_year,
    debut_year_distance_percent_rank,
    prev2_debut_year,
    debut_year_max_2,
    track_is_explicit_min_rows_in_track_id,
    COALESCE(track_mood, 'UNKNOWN') AS track_mood_or_unknown
FROM sequenced
ORDER BY track_mood_slug DESC
