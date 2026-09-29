WITH scoped AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        ADD_MONTHS(release_date, 6) AS release_date_plus_6m
    FROM tracks
    WHERE YEAR(release_date) = 2024
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        release_date_plus_6m,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit)) AS distinct_artist_id_in_is_explicit,
        MIN(duration_sec) OVER (PARTITION BY is_explicit) AS is_explicit_min_duration_sec,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY is_explicit), 0), 4) AS duration_sec_share_of_is_explicit,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit), 0), 4) AS bpm_share_of_is_explicit,
        ROW_NUMBER() OVER w_artist_id_by_duration_sec AS rn,
        MAX(bpm) OVER w_artist_id AS artist_id_max_bpm,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id ORDER BY duration_sec, id) AS duration_sec_dense_rank,
        DATEDIFF(release_date, LAG(release_date) OVER w_artist_id_release_date_year) AS days_since_prev_release_date,
        DATEDIFF(release_date_plus_6m, LAG(release_date_plus_6m) OVER w_artist_id_release_date_month) AS days_since_prev_release_date_plus_6m
    FROM scoped
    WINDOW w_artist_id_by_duration_sec AS (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id), w_artist_id AS (PARTITION BY artist_id), w_artist_id_release_date_year AS (PARTITION BY artist_id, YEAR(release_date) ORDER BY duration_sec ASC, id), w_artist_id_release_date_month AS (PARTITION BY artist_id, MONTH(release_date) ORDER BY bpm ASC, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        artist_id_max_bpm + duration_sec_dense_rank AS artist_id_max_bpm_plus_duration_sec_dense_rank,
        bpm_share_of_is_explicit + rn AS bpm_share_of_is_explicit_plus_rn
    FROM windowed
)
SELECT
    id,
    artist_id,
    title,
    is_explicit,
    bpm,
    distinct_artist_id_in_is_explicit,
    duration_sec_share_of_is_explicit,
    rn,
    artist_id_max_bpm,
    duration_sec_dense_rank,
    days_since_prev_release_date_plus_6m,
    artist_id_max_bpm_plus_duration_sec_dense_rank,
    rn - bpm_share_of_is_explicit AS rn_minus_bpm_share_of_is_explicit
FROM annotated
