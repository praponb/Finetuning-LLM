WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'party')
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit,
        DENSE_RANK() OVER w_all_by_bpm_ties AS bpm_dense_rank,
        ROW_NUMBER() OVER (ORDER BY duration_sec ASC, id) AS rn,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY is_explicit), 2) AS bpm_vs_is_explicit_avg,
        FIRST_VALUE(duration_sec) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        SUM(bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS bpm_sum_3_prior,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id, 0), 4) AS duration_sec_share_of_artist_id,
        LAG(duration_sec, 3, 0) OVER w_all_by_duration_sec AS prev3_duration_sec,
        FIRST_VALUE(bpm) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        SUM(duration_sec) OVER (PARTITION BY is_explicit) AS is_explicit_sum_duration_sec,
        ROW_NUMBER() OVER w_is_explicit_artist_id_by_dur AS duration_sec_seq
    FROM valid_tracks
    WINDOW w_all_by_bpm_ties AS (ORDER BY bpm DESC), w_artist_id AS (PARTITION BY artist_id), w_all_by_duration_sec AS (ORDER BY duration_sec ASC, id), w_is_explicit_artist_id_by_dur AS (PARTITION BY is_explicit, artist_id ORDER BY duration_sec DESC NULLS FIRST, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(first_duration_sec, 0) AS first_duration_sec_filled,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        ROUND(rows_in_is_explicit / NULLIF(duration_sec, 0), 3) AS rows_in_is_explicit_per_duration_sec
    FROM with_trends
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    moods,
    rows_in_is_explicit,
    bpm_dense_rank,
    first_duration_sec,
    bpm_sum_3_prior,
    duration_sec_share_of_artist_id,
    prev3_duration_sec,
    first_bpm,
    is_explicit_sum_duration_sec,
    duration_sec_seq,
    first_duration_sec_filled,
    is_explicit_flag,
    rows_in_is_explicit_per_duration_sec,
    CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref,
    ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text
FROM labeled
ORDER BY first_bpm DESC NULLS FIRST
