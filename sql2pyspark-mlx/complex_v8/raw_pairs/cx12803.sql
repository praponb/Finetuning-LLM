WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE title IS NOT NULL
      AND duration_sec > 154
),
joined AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        a.artist_name
    FROM recent_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.is_verified = FALSE
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        RANK() OVER (ORDER BY bpm DESC, id) AS bpm_rank,
        LAST_VALUE(release_date) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        LEAD(bpm, 2) OVER (ORDER BY bpm DESC, id) AS next2_bpm,
        LEAD(bpm) OVER (ORDER BY duration_sec NULLS LAST, id) AS next_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_bpm,
        SUM(bpm) OVER (ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_sum_6_prior,
        NTILE(10) OVER (ORDER BY bpm DESC, id) AS bpm_decile,
        ROUND(AVG(duration_sec) OVER w_artist_id, 2) AS artist_id_avg_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_sum_duration_sec,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm
    FROM joined
    WINDOW w_artist_id AS (PARTITION BY artist_id)
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    moods,
    second_duration_sec,
    bpm_rank,
    last_release_date,
    last_known_duration_sec,
    rows_in_artist_id,
    next2_bpm,
    next_bpm,
    artist_id_avg_bpm,
    bpm_sum_6_prior,
    bpm_decile,
    artist_id_avg_duration_sec,
    artist_id_sum_duration_sec
FROM sequenced
ORDER BY last_known_duration_sec DESC
