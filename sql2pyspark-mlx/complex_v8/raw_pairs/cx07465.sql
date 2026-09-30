WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE SIZE(moods) >= 1
),
combined_details AS (
    -- Join artists
    SELECT s.*, a.is_verified AS artist_is_verified, a.genre AS artist_genre
    FROM valid s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre = 'Pop'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(release_date) OVER (ORDER BY title_len NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date,
        COUNT(*) OVER (ORDER BY title_len NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(title_len / NULLIF(SUM(title_len) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS title_len_share_of_artist_id_is_explicit,
        ROUND(AVG(bpm) OVER (PARTITION BY title_len, artist_is_verified ORDER BY title_len DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_3,
        ROUND(AVG(title_len) OVER (PARTITION BY is_explicit, artist_is_verified ORDER BY bpm, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS title_len_avg_4,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY title_len NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY is_explicit, artist_is_verified ORDER BY bpm, id)) AS days_since_prev_release_date,
        SIZE(COLLECT_SET(artist_genre) OVER (PARTITION BY artist_id, is_explicit)) AS distinct_artist_genre_in_artist_id_is_explicit,
        SUM(bpm) OVER (PARTITION BY artist_is_verified ORDER BY title_len DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS bpm_sum_7,
        FIRST_VALUE(title_len) IGNORE NULLS OVER (PARTITION BY is_explicit, artist_is_verified ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_title_len,
        MAX(title_len) OVER (PARTITION BY artist_is_verified) AS artist_is_verified_max_title_len,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, artist_is_verified ORDER BY bpm, id) AS seq_no,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY is_explicit, artist_is_verified ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_is_verified), 0), 4) AS bpm_share_of_artist_is_verified,
        RANK() OVER w_title_len_artist_is_verified_2 AS title_len_rank,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_is_verified ORDER BY title_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm
    FROM combined_details
    WINDOW w_title_len_artist_is_verified_2 AS (PARTITION BY title_len, artist_is_verified ORDER BY title_len DESC)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_bpm_sum_7, 3) AS bpm_sum_7_cume_dist,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY artist_is_verified, artist_genre)) AS distinct_artist_id_in_artist_is_verified_artist_,
        LAG(last_release_date) OVER (ORDER BY bpm_sum_7 ASC, id) AS prev_last_release_date
    FROM windowed
    WINDOW w_all_by_bpm_sum_7 AS (ORDER BY bpm_sum_7 ASC, id)
)
-- Final projection
SELECT
    id,
    release_date,
    is_explicit,
    bpm,
    title_len,
    artist_genre,
    last_release_date,
    running_count,
    title_len_share_of_artist_id_is_explicit,
    bpm_avg_3,
    title_len_avg_4,
    first_known_bpm,
    bpm_sum_7,
    first_known_title_len,
    seq_no,
    second_release_date,
    bpm_share_of_artist_is_verified,
    title_len_rank,
    second_bpm,
    bpm_sum_7_cume_dist,
    distinct_artist_id_in_artist_is_verified_artist_,
    prev_last_release_date,
    DATEDIFF(release_date, second_release_date) AS days_second_release_date_to_release_date,
    DATEDIFF(DATE '2026-01-01', prev_last_release_date) AS days_since_prev_last_release_date
FROM with_windows
ORDER BY bpm_sum_7, release_date, id
LIMIT 10
