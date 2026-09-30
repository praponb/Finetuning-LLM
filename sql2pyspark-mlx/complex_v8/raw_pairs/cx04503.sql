WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'P%'
),
with_parent AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.moods,
        s.title_slug,
        s.bpm_bucket,
        a.genre AS artist_genre
    FROM recent_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre NOT IN ('HipHop', 'Classical')
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS first_track_release_date,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count
    FROM with_parent s
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_first_track_release_date_mon, 3) AS track_count_percent_rank,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY track_count), 0), 4) AS bpm_share_of_track_count,
        SUM(track_count) OVER (PARTITION BY track_count) AS track_count_sum_track_count,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', first_track_release_date), artist_genre ORDER BY track_count DESC) AS track_count_rank
    FROM with_stats
    WINDOW w_first_track_release_date_mon AS (PARTITION BY DATE_TRUNC('MONTH', first_track_release_date), artist_genre ORDER BY track_count DESC, id)
)
SELECT
    track_count_rank,
    artist_id,
    artist_genre,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(artist_genre)) AS artist_genre_list
FROM sequenced
GROUP BY GROUPING SETS ((track_count_rank, artist_id, artist_genre), (track_count_rank), ())
ORDER BY track_count_rank NULLS LAST, artist_id NULLS LAST, artist_genre NULLS LAST
