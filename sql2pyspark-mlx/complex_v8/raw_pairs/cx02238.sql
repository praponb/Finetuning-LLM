WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        ABS(bpm - 142) AS bpm_distance
    FROM tracks
    WHERE artist_id <= 14
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        title_len,
        LAST_VALUE(bpm_distance) IGNORE NULLS OVER (PARTITION BY title_len, is_explicit ORDER BY title_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm_distance,
        LAG(duration_sec, 2) OVER (ORDER BY bpm, id) AS prev2_duration_sec,
        LEAD(title_len, 2) OVER w_release_date_month_by_title AS next2_title_len,
        ROUND(AVG(title_len) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY title_len, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS title_len_avg_7,
        title_len - LAG(title_len) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY title_len DESC NULLS FIRST, id) AS title_len_change,
        LAG(bpm_distance) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY title_len, id) AS prev_bpm_distance,
        SUM(title_len) OVER (PARTITION BY title_len, is_explicit ORDER BY title_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_title_len,
        SUM(title_len) OVER (PARTITION BY title_len, artist_id) AS title_len_artist_id_sum_title_len,
        LAST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY title_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        MAX(title_len) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY title_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_title_len_so_far,
        SUM(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY title_len DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec
    FROM recent
    WINDOW w_release_date_month_by_title AS (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY title_len, id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY next2_title_len, YEAR(release_date) ORDER BY max_title_len_so_far DESC, id), 3) AS max_title_len_so_far_cume_dist,
        ROUND(last_known_bpm_distance - AVG(last_known_bpm_distance) OVER w_last_known_release_date_mont, 2) AS last_known_bpm_distance_vs_last_known_release_da,
        RANK() OVER w_next2_title_len_release_date_2 AS max_title_len_so_far_rank,
        FIRST_VALUE(max_title_len_so_far) IGNORE NULLS OVER (PARTITION BY MONTH(last_known_release_date) ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_title_len_so_far,
        MIN(next2_title_len) OVER w_artist_id_title_len AS artist_id_title_len_min_next2_title_len
    FROM windowed
    WINDOW w_last_known_release_date_mont AS (PARTITION BY MONTH(last_known_release_date)), w_next2_title_len_release_date_2 AS (PARTITION BY next2_title_len, YEAR(release_date) ORDER BY max_title_len_so_far DESC), w_artist_id_title_len AS (PARTITION BY artist_id, title_len)
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    moods,
    title_len,
    last_known_bpm_distance,
    title_len_avg_7,
    prev_bpm_distance,
    last_known_release_date,
    running_duration_sec,
    last_known_bpm_distance_vs_last_known_release_da,
    max_title_len_so_far_rank,
    first_known_max_title_len_so_far,
    artist_id_title_len_min_next2_title_len,
    YEAR(last_known_release_date) AS last_known_release_date_year
FROM sequenced
WHERE running_duration_sec BETWEEN 1323 AND 3789
ORDER BY artist_id_title_len_min_next2_title_len DESC NULLS FIRST, max_title_len_so_far_rank DESC, id
LIMIT 5
