WITH base AS (
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
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text
    FROM tracks
    WHERE is_explicit = FALSE
      AND duration_sec <= 353
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods,
        ROUND(CUME_DIST() OVER w_is_explicit_release_date_yea, 3) AS bpm_cume_dist,
        MAX(bpm) OVER (PARTITION BY is_explicit, YEAR(release_date) ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_so_far,
        ROW_NUMBER() OVER (ORDER BY bpm ASC, id) AS bpm_seq,
        LEAD(bpm, 2) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id) AS next2_bpm,
        RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec) AS duration_sec_rank,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id, YEAR(release_date)), 2) AS bpm_vs_artist_id_release_date_year_avg,
        LAST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        duration_sec - LAG(duration_sec) OVER w_artist_id_by_release_date AS duration_sec_change,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_bpm,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg,
        ROUND(AVG(bpm) OVER (ORDER BY bpm ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_4,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        DENSE_RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec) AS duration_sec_dense_rank,
        COUNT(*) OVER (PARTITION BY is_explicit, MONTH(release_date)) AS rows_in_is_explicit_release_date_month
    FROM base
    WINDOW w_is_explicit_release_date_yea AS (PARTITION BY is_explicit, YEAR(release_date) ORDER BY bpm, id), w_artist_id_by_release_date AS (PARTITION BY artist_id ORDER BY release_date DESC, id)
)
SELECT
    id,
    duration_sec,
    is_explicit,
    moods,
    max_bpm_so_far,
    next2_bpm,
    bpm_vs_artist_id_release_date_year_avg,
    last_known_bpm,
    artist_id_avg_bpm,
    duration_sec_vs_artist_id_avg,
    bpm_avg_4,
    second_duration_sec,
    duration_sec_dense_rank,
    rows_in_is_explicit_release_date_month,
    IF(is_explicit, 'yes', 'no') AS is_explicit_label
FROM windowed
WHERE duration_sec_dense_rank <= 8
ORDER BY is_explicit_label DESC
