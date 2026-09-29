WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE release_date >= DATE '2024-06-03'
),
enriched_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.title_prefix,
        s.title_slug,
        a.country AS artist_country,
        a.genre AS artist_genre
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre = 'HipHop'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_genre, 2) AS duration_sec_vs_artist_genre_avg,
        SUM(duration_sec) OVER (ORDER BY release_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_sum_3,
        LEAD(release_date, 3) OVER (PARTITION BY artist_genre ORDER BY duration_sec ASC, id) AS next3_release_date,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_country ORDER BY bpm DESC), 3) AS bpm_cume_dist
    FROM enriched_artist
    WINDOW w_artist_genre AS (PARTITION BY artist_genre)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        bpm,
        moods,
        title_prefix,
        title_slug,
        artist_country,
        artist_genre,
        running_count,
        bpm_cume_dist,
        RANK() OVER (ORDER BY duration_sec_vs_artist_genre_avg DESC) AS duration_sec_vs_artist_genre_avg_rank,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', next3_release_date), MONTH(release_date) ORDER BY bpm_cume_dist NULLS LAST, id) AS bpm_cume_dist_seq,
        SUM(bpm_cume_dist) OVER (PARTITION BY is_explicit, artist_country) AS is_explicit_artist_country_sum_bpm_cume_dist,
        SIZE(COLLECT_SET(artist_genre) OVER (PARTITION BY is_explicit, artist_country)) AS distinct_artist_genre_in_is_explicit_artist_coun
    FROM with_windows
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE duration_sec_vs_artist_genre_avg_rank > (SELECT AVG(duration_sec_vs_artist_genre_avg_rank) FROM metrics)
),
qualified_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE bpm_cume_dist > (SELECT AVG(bpm_cume_dist) FROM qualified)
)
SELECT id, metric_name, amount
FROM (
    SELECT id, bpm, running_count
    FROM qualified_2
) src
UNPIVOT (
    amount FOR metric_name IN (bpm, running_count)
) unp
