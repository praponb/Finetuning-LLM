WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'focus')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        is_explicit_flag,
        ROUND(title_len / NULLIF(SUM(title_len) OVER (PARTITION BY artist_id, title_len), 0), 4) AS title_len_share_of_artist_id_title_len,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY is_explicit_flag, artist_id ORDER BY title_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_artist_id_title_len, 0), 4) AS bpm_share_of_artist_id_title_len,
        DENSE_RANK() OVER (PARTITION BY is_explicit_flag, artist_id ORDER BY title_len) AS title_len_dense_rank,
        MAX(is_explicit_flag) OVER (PARTITION BY is_explicit_flag, artist_id ORDER BY title_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_is_explicit_flag_so_far
    FROM recent
    WINDOW w_artist_id_title_len AS (PARTITION BY artist_id, title_len)
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT artist_id, SUM(duration_sec) AS total_track_duration_sec
    FROM tracks
    GROUP BY artist_id
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.total_track_duration_sec
    FROM sequenced s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
with_stats_2 AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count
    FROM with_stats s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
with_activity AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM with_stats_2 s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND UPPER(TRIM(t.title)) LIKE 'N%'
        )
)
-- Final projection
SELECT
    id,
    artist_id,
    is_explicit,
    title_len_share_of_artist_id_title_len,
    first_known_bpm,
    bpm_share_of_artist_id_title_len,
    title_len_dense_rank,
    play_count,
    COALESCE(total_track_duration_sec, 0) AS total_track_duration_sec_filled,
    LAST_DAY(release_date) AS release_date_month_end
FROM with_activity
