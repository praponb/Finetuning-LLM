WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec
    FROM tracks
    WHERE bpm BETWEEN 73 AND 166
      AND artist_id >= 9
),
enriched AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.least_bpm_duration_sec,
        a.artist_name,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY artist_genre) AS rows_in_artist_genre,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_genre, artist_is_verified ORDER BY release_date ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_3_prior,
        MAX(least_bpm_duration_sec) OVER (ORDER BY release_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS least_bpm_duration_sec_max_7,
        ROW_NUMBER() OVER (PARTITION BY artist_genre, artist_is_verified ORDER BY release_date ASC, id) AS seq_no,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit), 2) AS is_explicit_avg_bpm,
        DENSE_RANK() OVER w_is_explicit_release_date_yea AS release_date_dense_rank,
        DENSE_RANK() OVER (PARTITION BY artist_genre, artist_is_verified ORDER BY release_date ASC) AS release_date_dense_rank_2,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit), 0), 4) AS bpm_share_of_is_explicit,
        ROW_NUMBER() OVER w_all_by_release_date AS seq_no_2,
        RANK() OVER (PARTITION BY is_explicit, YEAR(release_date) ORDER BY release_date) AS release_date_rank
    FROM enriched
    WINDOW w_is_explicit_release_date_yea AS (PARTITION BY is_explicit, YEAR(release_date) ORDER BY release_date), w_all_by_release_date AS (ORDER BY release_date DESC, id)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        least_bpm_duration_sec,
        artist_is_verified,
        artist_genre,
        least_bpm_duration_sec_max_7,
        seq_no,
        is_explicit_avg_bpm,
        release_date_dense_rank,
        release_date_dense_rank_2,
        bpm_share_of_is_explicit,
        seq_no_2,
        release_date_rank,
        INITCAP(TRIM(artist_genre)) AS artist_genre_clean
    FROM ranked
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(seq_no_2) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY release_date_dense_rank, id) AS next_seq_no_2,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_is_verified, artist_id ORDER BY release_date_dense_rank), 3) AS release_date_dense_rank_cume_dist,
        SIZE(COLLECT_SET(artist_id) OVER w_release_date_dense_rank_2_ar) AS distinct_artist_id_in_release_date_dense_rank_2_,
        SUM(release_date_dense_rank_2) OVER w_release_date_dense_rank_2_ar AS release_date_dense_rank_2_artist_genre_sum_relea,
        DENSE_RANK() OVER (PARTITION BY artist_is_verified, artist_id ORDER BY release_date_dense_rank, id) AS release_date_dense_rank_dense_rank,
        COUNT(*) OVER (PARTITION BY artist_id, seq_no_2 ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM scored
    WINDOW w_release_date_dense_rank_2_ar AS (PARTITION BY release_date_dense_rank_2, artist_genre)
)
SELECT
    id,
    duration_sec,
    bpm,
    artist_is_verified,
    artist_genre,
    is_explicit_avg_bpm,
    release_date_dense_rank,
    release_date_dense_rank_2,
    bpm_share_of_is_explicit,
    seq_no_2,
    artist_genre_clean,
    next_seq_no_2,
    release_date_dense_rank_cume_dist,
    distinct_artist_id_in_release_date_dense_rank_2_,
    release_date_dense_rank_dense_rank,
    running_count
FROM with_trends
