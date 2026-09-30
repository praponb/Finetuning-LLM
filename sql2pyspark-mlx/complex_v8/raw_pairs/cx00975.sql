WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'focus')
      AND LOWER(title) LIKE '%ts%'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        ROUND(CUME_DIST() OVER w_is_explicit_is_explicit_labe, 3) AS duration_sec_filled_cume_dist,
        ROUND(duration_sec_filled / NULLIF(SUM(duration_sec_filled) OVER (PARTITION BY artist_id), 0), 4) AS duration_sec_filled_share_of_artist_id,
        LEAD(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm, id) AS next_duration_sec,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id_is_explicit, 2) AS duration_sec_vs_artist_id_is_explicit_avg,
        duration_sec_filled - LAG(duration_sec_filled) OVER w_is_explicit_label_by_bpm AS duration_sec_filled_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit_label ORDER BY duration_sec, id), 3) AS duration_sec_percent_rank,
        FIRST_VALUE(bpm) OVER (ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        LEAD(duration_sec, 3) OVER (ORDER BY bpm ASC, id) AS next3_duration_sec,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        ROUND(AVG(bpm) OVER w_artist_id_is_explicit, 2) AS artist_id_is_explicit_avg_bpm,
        LAST_VALUE(duration_sec_filled) OVER (PARTITION BY is_explicit, is_explicit_label ORDER BY duration_sec_filled ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec_filled,
        NTH_VALUE(duration_sec_filled, 2) OVER (PARTITION BY is_explicit_label ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_filled,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id, 2) AS duration_sec_vs_artist_id_avg,
        SUM(duration_sec_filled) OVER w_artist_id_is_explicit AS artist_id_is_explicit_sum_duration_sec_filled,
        SUM(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_sum_3
    FROM valid
    WINDOW w_is_explicit_is_explicit_labe AS (PARTITION BY is_explicit, is_explicit_label ORDER BY duration_sec_filled ASC NULLS LAST), w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_is_explicit_label_by_bpm AS (PARTITION BY is_explicit_label ORDER BY bpm, id), w_artist_id AS (PARTITION BY artist_id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        duration_sec + last_duration_sec_filled AS duration_sec_plus_last_duration_sec_filled
    FROM ranked
    WHERE duration_sec_sum_3 < 734
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    moods,
    duration_sec_filled_cume_dist,
    duration_sec_filled_share_of_artist_id,
    next_duration_sec,
    duration_sec_vs_artist_id_is_explicit_avg,
    duration_sec_filled_change,
    next3_duration_sec,
    rows_in_artist_id,
    last_duration_sec_filled,
    second_duration_sec_filled,
    duration_sec_vs_artist_id_avg,
    artist_id_is_explicit_sum_duration_sec_filled,
    duration_sec_sum_3,
    is_explicit_flag,
    duration_sec_plus_last_duration_sec_filled
FROM derived
ORDER BY duration_sec_filled_share_of_artist_id
