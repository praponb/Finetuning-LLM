WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE duration_sec < 402
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id AND a.genre = 'Classical'
    WHERE a.is_verified = FALSE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        artist_debut_year,
        LAG(artist_debut_year, 2) OVER w_artist_id_is_explicit_by_dur AS prev2_artist_debut_year,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id_artist_is_verified, 0), 4) AS duration_sec_share_of_artist_id_artist_is_verifi,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_is_verified ORDER BY duration_sec ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS artist_debut_year_avg_6_prior,
        MAX(artist_debut_year) OVER (PARTITION BY artist_is_verified ORDER BY duration_sec ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS artist_debut_year_max_1_prior,
        FIRST_VALUE(artist_debut_year) IGNORE NULLS OVER (PARTITION BY artist_is_verified ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_artist_debut_year,
        LAG(artist_debut_year) OVER (PARTITION BY artist_is_verified ORDER BY release_date, id) AS prev_artist_debut_year,
        COUNT(*) OVER (PARTITION BY artist_id, artist_is_verified) AS rows_in_artist_id_artist_is_verified,
        MAX(duration_sec) OVER (PARTITION BY artist_is_verified ORDER BY release_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_max_3,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY MONTH(release_date) ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY artist_is_verified ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_so_far,
        MAX(duration_sec) OVER (PARTITION BY is_explicit) AS is_explicit_max_duration_sec,
        LAST_VALUE(artist_debut_year) IGNORE NULLS OVER (PARTITION BY artist_is_verified ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_artist_debut_year,
        LEAD(duration_sec, 2) OVER (PARTITION BY artist_is_verified ORDER BY duration_sec ASC, id) AS next2_duration_sec,
        MAX(artist_debut_year) OVER (ORDER BY artist_debut_year ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS artist_debut_year_max_4,
        ROUND(AVG(duration_sec) OVER (PARTITION BY is_explicit), 2) AS is_explicit_avg_duration_sec
    FROM combined_details_artist
    WINDOW w_artist_id_is_explicit_by_dur AS (PARTITION BY artist_id, is_explicit ORDER BY duration_sec DESC, id), w_artist_id_artist_is_verified AS (PARTITION BY artist_id, artist_is_verified)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        artist_debut_year,
        prev2_artist_debut_year,
        duration_sec_share_of_artist_id_artist_is_verifi,
        artist_debut_year_avg_6_prior,
        prev_artist_debut_year,
        duration_sec_max_3,
        max_duration_sec_so_far,
        is_explicit_max_duration_sec,
        last_known_artist_debut_year,
        artist_debut_year_max_4,
        is_explicit_avg_duration_sec,
        ABS(prev_artist_debut_year - 2015) AS prev_artist_debut_year_distance,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM with_windows
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec_share_of_artist_id_artist_is_verifi,
    artist_debut_year_avg_6_prior,
    prev_artist_debut_year,
    duration_sec_max_3,
    max_duration_sec_so_far,
    is_explicit_max_duration_sec,
    last_known_artist_debut_year,
    title_prefix,
    title_slug
FROM scored
ORDER BY last_known_artist_debut_year DESC
