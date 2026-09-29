WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        QUARTER(release_date) AS release_date_quarter
    FROM tracks
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.id) AS first_played_at
    FROM prepared_tracks s
),
artist_id_stats AS (
    -- Avg play_count per artist_id
    SELECT artist_id, AVG(play_count) AS avg_play_count
    FROM summarized_rows
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_play_count,
        ROUND(s.play_count - g.avg_play_count, 2) AS play_count_minus_avg
    FROM summarized_rows s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.play_count > g.avg_play_count
),
screened AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM compared s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND LOWER(a.artist_name) LIKE '% l%'
        )
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        title,
        release_date,
        is_explicit,
        moods,
        release_date_month,
        play_count,
        first_played_at,
        avg_play_count,
        ROUND(AVG(play_count_minus_avg) OVER (PARTITION BY release_date_quarter, is_explicit ORDER BY release_date DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS play_count_minus_avg_avg_4,
        DENSE_RANK() OVER (PARTITION BY release_date_quarter, is_explicit ORDER BY release_date DESC NULLS FIRST, id) AS release_date_dense_rank,
        FIRST_VALUE(release_date_month) IGNORE NULLS OVER (PARTITION BY release_date_quarter, is_explicit ORDER BY release_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date_month,
        LAST_VALUE(first_played_at) OVER (PARTITION BY release_date_quarter, is_explicit ORDER BY release_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_played_at,
        ROUND(AVG(release_date_quarter) OVER w_release_date_quarter_artist, 2) AS release_date_quarter_artist_id_avg_release_date_
    FROM screened
    WINDOW w_release_date_quarter_artist AS (PARTITION BY release_date_quarter, artist_id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        release_date,
        is_explicit,
        release_date_month,
        play_count,
        first_played_at,
        avg_play_count,
        play_count_minus_avg_avg_4,
        release_date_dense_rank,
        first_known_release_date_month,
        release_date_quarter_artist_id_avg_release_date_,
        ROUND(AVG(play_count_minus_avg_avg_4) OVER w_release_date_dense_rank_is_e, 2) AS release_date_dense_rank_is_explicit_avg_play_cou,
        ROW_NUMBER() OVER (PARTITION BY is_explicit ORDER BY release_date_month, id) AS rn,
        MAX(play_count) OVER w_release_date_dense_rank_is_e AS release_date_dense_rank_is_explicit_max_play_cou
    FROM with_trends
    WINDOW w_release_date_dense_rank_is_e AS (PARTITION BY release_date_dense_rank, is_explicit)
)
-- Final projection
SELECT
    id,
    release_date,
    is_explicit,
    release_date_month,
    play_count,
    first_played_at,
    avg_play_count,
    play_count_minus_avg_avg_4,
    release_date_dense_rank,
    first_known_release_date_month,
    release_date_quarter_artist_id_avg_release_date_,
    rn,
    release_date_dense_rank_is_explicit_max_play_cou,
    FLOOR(release_date_dense_rank / 2) * 2 AS release_date_dense_rank_bucket
FROM metrics
ORDER BY first_played_at DESC, release_date_dense_rank_bucket, id
LIMIT 10
