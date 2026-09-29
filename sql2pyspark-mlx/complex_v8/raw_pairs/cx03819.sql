WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE release_date IS NOT NULL
      AND duration_sec <= 352
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        moods,
        bpm - LAG(bpm) OVER (ORDER BY duration_sec DESC, id) AS bpm_change,
        MIN(duration_sec) OVER w_artist_id AS artist_id_min_duration_sec,
        MAX(duration_sec) OVER w_artist_id AS artist_id_max_duration_sec,
        RANK() OVER w_is_explicit_artist_id_by_bpm AS bpm_rank,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm NULLS LAST), 3) AS bpm_percent_rank,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec
    FROM cleaned_tracks
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_is_explicit_artist_id_by_bpm AS (PARTITION BY is_explicit, artist_id ORDER BY bpm NULLS LAST, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        artist_id,
        title,
        moods,
        bpm_change,
        artist_id_min_duration_sec,
        artist_id_max_duration_sec,
        rows_in_artist_id,
        duration_sec_vs_artist_id_avg,
        bpm_percent_rank,
        last_known_duration_sec
    FROM windowed
    WHERE bpm_rank <= 5
),
per_group AS (
    -- Summarise per group
    SELECT
        artist_id,
        SUM(CASE WHEN UPPER(TRIM(title)) LIKE 'R%' THEN bpm_change ELSE 0 END) AS bpm_change_upper_trim_title_like_r,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        ROUND(SUM(last_known_duration_sec) / NULLIF(SUM(rows_in_artist_id), 0), 3) AS last_known_duration_sec_to_rows_in_artist_id_rat,
        MAX(rows_in_artist_id) AS max_rows_in_artist_id,
        MIN(bpm_percent_rank) AS min_bpm_percent_rank
    FROM first_rows
    GROUP BY 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY max_rows_in_artist_id DESC NULLS FIRST) AS max_rows_in_artist_id_dense_rank,
        min_bpm_percent_rank - LAG(min_bpm_percent_rank) OVER (PARTITION BY artist_id ORDER BY max_rows_in_artist_id DESC NULLS FIRST, artist_id) AS min_bpm_percent_rank_change
    FROM per_group
)
SELECT
    artist_id,
    bpm_change_upper_trim_title_like_r,
    last_known_duration_sec_to_rows_in_artist_id_rat,
    max_rows_in_artist_id,
    max_rows_in_artist_id_dense_rank,
    min_bpm_percent_rank_change,
    GREATEST(max_rows_in_artist_id, min_bpm_percent_rank) AS greatest_max_rows_in_artist_id_min_bpm_percent_r,
    ROUND(min_bpm_percent_rank / NULLIF(max_rows_in_artist_id, 0), 3) AS min_bpm_percent_rank_per_max_rows_in_artist_id
FROM ranked
ORDER BY last_known_duration_sec_to_rows_in_artist_id_rat NULLS LAST, min_bpm_percent_rank_per_max_rows_in_artist_id DESC
