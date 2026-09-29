WITH filtered_tracks AS (
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
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
    FROM tracks
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.is_explicit_flag,
        a.country AS artist_country,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM filtered_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_parent_artist s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(is_explicit_flag) OVER (PARTITION BY artist_is_verified, artist_id) AS artist_is_verified_artist_id_min_is_explicit_fla,
        LAST_VALUE(bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        SUM(is_explicit_flag) OVER (PARTITION BY artist_is_verified, artist_id) AS artist_is_verified_artist_id_sum_is_explicit_fla,
        LEAD(duration_sec) OVER (PARTITION BY is_explicit_flag ORDER BY duration_sec, id) AS next_duration_sec,
        ROUND(bpm - AVG(bpm) OVER w_artist_is_verified_artist_id, 2) AS bpm_vs_artist_is_verified_artist_id_avg
    FROM screened
    WINDOW w_artist_is_verified_artist_id AS (PARTITION BY artist_is_verified, artist_id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY artist_is_verified_artist_id_sum_is_explicit_fla, is_explicit_flag ORDER BY artist_is_verified_artist_id_sum_is_explicit_fla, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY artist_is_verified, is_explicit_flag ORDER BY artist_is_verified_artist_id_sum_is_explicit_fla, id) AS artist_is_verified_artist_id_sum_is_explicit_fla_2,
        RANK() OVER (PARTITION BY is_explicit ORDER BY bpm_vs_artist_is_verified_artist_id_avg DESC) AS bpm_vs_artist_is_verified_artist_id_avg_rank,
        SIZE(COLLECT_SET(artist_country) OVER w_artist_is_verified_artist_id_3) AS distinct_artist_country_in_artist_is_verified_ar,
        bpm - LAG(bpm) OVER (PARTITION BY artist_is_verified_artist_id_sum_is_explicit_fla, is_explicit_flag ORDER BY artist_is_verified_artist_id_sum_is_explicit_fla, id) AS bpm_change
    FROM with_trends
    WINDOW w_artist_is_verified_artist_id_3 AS (PARTITION BY artist_is_verified_artist_id_sum_is_explicit_fla)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        artist_country,
        artist_is_verified_artist_id_sum_is_explicit_fla,
        bpm_vs_artist_is_verified_artist_id_avg,
        seq_no,
        bpm_vs_artist_is_verified_artist_id_avg_rank,
        distinct_artist_country_in_artist_is_verified_ar,
        bpm_change,
        next_duration_sec - LAG(next_duration_sec) OVER (PARTITION BY artist_is_verified_artist_id_sum_is_explicit_fla, is_explicit ORDER BY seq_no DESC, id) AS next_duration_sec_change,
        RANK() OVER w_artist_genre_seq_no_by_seq_n AS seq_no_rank,
        ROW_NUMBER() OVER (PARTITION BY artist_genre, seq_no ORDER BY seq_no, id) AS seq_no_seq
    FROM ranked
    WINDOW w_artist_genre_seq_no_by_seq_n AS (PARTITION BY artist_genre, seq_no ORDER BY seq_no)
)
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    artist_country,
    artist_is_verified_artist_id_sum_is_explicit_fla,
    bpm_vs_artist_is_verified_artist_id_avg,
    seq_no,
    bpm_vs_artist_is_verified_artist_id_avg_rank,
    distinct_artist_country_in_artist_is_verified_ar,
    bpm_change,
    next_duration_sec_change,
    seq_no_rank,
    seq_no_seq,
    CONCAT_WS('|', artist_country, title) AS artist_country_title_label,
    LENGTH(TRIM(artist_country)) AS artist_country_len
FROM metrics
WHERE title IS NOT NULL
ORDER BY distinct_artist_country_in_artist_is_verified_ar, seq_no_rank
