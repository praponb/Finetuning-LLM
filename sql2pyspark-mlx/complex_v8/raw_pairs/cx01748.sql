WITH prepared_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        genre,
        country,
        is_verified,
        CASE WHEN genre IN ('Pop', 'Jazz') THEN 'group_a' WHEN genre = 'HipHop' THEN 'group_b' ELSE 'other' END AS genre_group
    FROM artists
    WHERE NOT is_verified
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT artist_id, MIN(duration_sec) AS min_track_duration_sec
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.min_track_duration_sec
    FROM prepared_artists s
    LEFT JOIN track_stats t ON t.artist_id = s.id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY is_verified, genre) AS rows_in_is_verified_genre,
        NTH_VALUE(min_track_duration_sec, 2) OVER (PARTITION BY is_verified ORDER BY min_track_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_track_duration_sec,
        SUM(min_track_duration_sec) OVER (PARTITION BY is_verified, genre) AS is_verified_genre_sum_min_track_duration_sec
    FROM with_counts
)
SELECT *
FROM (
    SELECT country, is_verified, genre_group, rows_in_is_verified_genre
    FROM metrics
) src
PIVOT (
    SUM(rows_in_is_verified_genre)
    FOR genre_group IN ('other', 'group_a')
)
