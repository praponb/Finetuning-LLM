WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE duration_sec < 365
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_3_prior,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit), 0), 4) AS bpm_share_of_is_explicit,
        ROUND(PERCENT_RANK() OVER w_is_explicit_by_duration_sec_2, 3) AS duration_sec_percent_rank,
        MIN(duration_sec) OVER (PARTITION BY artist_id, is_explicit) AS artist_id_is_explicit_min_duration_sec,
        bpm - LAG(bpm) OVER w_artist_id_is_explicit_by_bpm AS bpm_change,
        LEAD(duration_sec, 1, 0) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm NULLS LAST, id) AS next_duration_sec,
        SUM(bpm) OVER (PARTITION BY is_explicit) AS is_explicit_sum_bpm,
        DENSE_RANK() OVER (ORDER BY bpm) AS bpm_dense_rank
    FROM prepared_tracks
    WINDOW w_is_explicit_by_duration_sec_2 AS (PARTITION BY is_explicit ORDER BY duration_sec ASC), w_artist_id_is_explicit_by_bpm AS (PARTITION BY artist_id, is_explicit ORDER BY bpm NULLS LAST, id)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        title_slug,
        duration_sec_avg_3_prior,
        bpm_share_of_is_explicit,
        duration_sec_percent_rank,
        artist_id_is_explicit_min_duration_sec,
        bpm_change,
        next_duration_sec,
        is_explicit_sum_bpm
    FROM with_windows
    WHERE bpm_dense_rank <= 3
      AND bpm >= 94
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(bpm, 2, 0) OVER w_is_explicit_artist_id_by_is AS next2_bpm,
        NTILE(3) OVER (PARTITION BY is_explicit, artist_id ORDER BY is_explicit_sum_bpm DESC, id) AS is_explicit_sum_bpm_tercile,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, artist_id ORDER BY is_explicit_sum_bpm ASC, id) AS rn,
        MIN(duration_sec) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_min_duration_sec,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY is_explicit_sum_bpm ASC, id) AS duration_sec_change,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id) AS rows_in_is_explicit_artist_id
    FROM leaders
    WINDOW w_is_explicit_artist_id_by_is AS (PARTITION BY is_explicit, artist_id ORDER BY is_explicit_sum_bpm ASC, id)
)
SELECT
    id,
    bpm,
    title_slug,
    bpm_share_of_is_explicit,
    duration_sec_percent_rank,
    artist_id_is_explicit_min_duration_sec,
    bpm_change,
    next_duration_sec,
    is_explicit_sum_bpm,
    next2_bpm,
    is_explicit_sum_bpm_tercile,
    rn,
    rows_in_is_explicit_artist_id,
    UPPER(TRIM(title_slug)) AS title_slug_clean,
    INITCAP(TRIM(title_slug)) AS title_slug_clean_2
FROM with_trends
ORDER BY id DESC, is_explicit_sum_bpm DESC NULLS FIRST
LIMIT 20
