WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm
    FROM tracks
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_duration_sec, 3) AS duration_sec_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec_per_bpm), 3) AS duration_sec_per_bpm_percent_rank,
        MAX(bpm) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_max_bpm,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(bpm) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_min_bpm,
        MIN(bpm) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_bpm_so_far,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec ASC, id) AS rn,
        ROUND(duration_sec_per_bpm - AVG(duration_sec_per_bpm) OVER w_is_explicit_artist_id, 2) AS duration_sec_per_bpm_vs_is_explicit_artist_id_av,
        ROUND(AVG(duration_sec_per_bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_sec_per_bpm_avg_7,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit ORDER BY bpm, id), 3) AS bpm_percent_rank,
        NTH_VALUE(duration_sec_per_bpm, 2) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_per_bpm,
        bpm - LAG(bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id) AS bpm_change,
        MIN(duration_sec) OVER (ORDER BY duration_sec_per_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_so_far,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit, artist_id), 2) AS is_explicit_artist_id_avg_bpm,
        ROUND(PERCENT_RANK() OVER w_is_explicit_by_bpm_ties, 3) AS bpm_percent_rank_2,
        DENSE_RANK() OVER (ORDER BY duration_sec_per_bpm) AS duration_sec_per_bpm_dense_rank
    FROM filtered_tracks
    WINDOW w_artist_id_by_duration_sec AS (PARTITION BY artist_id ORDER BY duration_sec DESC, id), w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id), w_is_explicit_by_bpm_ties AS (PARTITION BY is_explicit ORDER BY bpm)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        title_slug,
        duration_sec_per_bpm,
        duration_sec_percent_rank,
        duration_sec_per_bpm_percent_rank,
        is_explicit_artist_id_max_bpm,
        running_count,
        is_explicit_artist_id_min_bpm,
        min_bpm_so_far,
        rn,
        duration_sec_per_bpm_vs_is_explicit_artist_id_av,
        duration_sec_per_bpm_avg_7,
        bpm_percent_rank,
        second_duration_sec_per_bpm,
        bpm_change,
        min_duration_sec_so_far,
        is_explicit_artist_id_avg_bpm,
        bpm_percent_rank_2
    FROM with_trends
    WHERE duration_sec_per_bpm_dense_rank = 1
      AND bpm BETWEEN 73 AND 156
)
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    duration_sec_per_bpm_percent_rank,
    is_explicit_artist_id_max_bpm,
    is_explicit_artist_id_min_bpm,
    min_bpm_so_far,
    rn,
    duration_sec_per_bpm_vs_is_explicit_artist_id_av,
    duration_sec_per_bpm_avg_7,
    bpm_percent_rank,
    bpm_change,
    min_duration_sec_so_far,
    is_explicit_artist_id_avg_bpm,
    bpm_percent_rank_2,
    FLOOR(duration_sec_per_bpm / 2) * 2 AS duration_sec_per_bpm_bucket
FROM top_per_group
ORDER BY min_duration_sec_so_far DESC NULLS FIRST
