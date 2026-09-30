WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm - duration_sec AS bpm_minus_duration_sec,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec
    FROM tracks
    WHERE (bpm < 147 OR NOT ARRAY_CONTAINS(moods, 'party'))
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm_minus_duration_sec,
        least_bpm_duration_sec,
        LEAD(bpm_minus_duration_sec, 2) OVER w_is_explicit_artist_id_by_rel AS next2_bpm_minus_duration_sec,
        NTH_VALUE(least_bpm_duration_sec, 2) OVER (ORDER BY least_bpm_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_least_bpm_duration_sec,
        ROW_NUMBER() OVER w_artist_id_is_explicit_by_rel AS seq_no,
        MAX(bpm_minus_duration_sec) OVER (ORDER BY least_bpm_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_minus_duration_sec_so_far,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit, MONTH(release_date))) AS distinct_artist_id_in_is_explicit_release_date_m,
        ROW_NUMBER() OVER (ORDER BY least_bpm_duration_sec, id) AS rn,
        LEAD(release_date, 3) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC, id) AS next3_release_date,
        LAG(bpm_minus_duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date, id) AS prev_bpm_minus_duration_sec
    FROM cleaned
    WINDOW w_is_explicit_artist_id_by_rel AS (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC, id), w_artist_id_is_explicit_by_rel AS (PARTITION BY artist_id, is_explicit ORDER BY release_date, id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY seq_no) AS rows_in_seq_no,
        next2_bpm_minus_duration_sec - LAG(next2_bpm_minus_duration_sec) OVER w_distinct_artist_id_in_is_exp AS next2_bpm_minus_duration_sec_change,
        MAX(next2_bpm_minus_duration_sec) OVER (PARTITION BY is_explicit, seq_no ORDER BY rn DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_next2_bpm_minus_duration_sec_so_far,
        SIZE(COLLECT_SET(artist_id) OVER w_seq_no) AS distinct_artist_id_in_seq_no,
        ROUND(PERCENT_RANK() OVER w_rn_is_explicit_by_seq_no, 3) AS seq_no_percent_rank,
        LAG(prev_bpm_minus_duration_sec) OVER (PARTITION BY rn, is_explicit ORDER BY seq_no, id) AS prev_prev_bpm_minus_duration_sec,
        ROUND(next2_bpm_minus_duration_sec - AVG(next2_bpm_minus_duration_sec) OVER (PARTITION BY seq_no), 2) AS next2_bpm_minus_duration_sec_vs_seq_no_avg
    FROM windowed
    WINDOW w_distinct_artist_id_in_is_exp AS (PARTITION BY distinct_artist_id_in_is_explicit_release_date_m, artist_id ORDER BY distinct_artist_id_in_is_explicit_release_date_m, id), w_seq_no AS (PARTITION BY seq_no), w_rn_is_explicit_by_seq_no AS (PARTITION BY rn, is_explicit ORDER BY seq_no, id)
),
totals AS (
    -- Summarise per group
    SELECT
        is_explicit,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list
    FROM ranked
    GROUP BY is_explicit
)
-- Final projection
SELECT DISTINCT is_explicit
FROM totals
