WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        INITCAP(TRIM(title)) AS title_clean
    FROM tracks
    WHERE release_date < DATE '2025-04-02'
      AND UPPER(TRIM(title)) LIKE 'S%'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        moods,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date DESC NULLS FIRST), 3) AS release_date_percent_rank,
        LEAD(release_date) OVER w_is_explicit_by_bpm AS next_release_date,
        MAX(bpm) OVER w_is_explicit_artist_id AS is_explicit_artist_id_max_bpm,
        LEAD(release_date) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date DESC NULLS FIRST, id) AS next_release_date_2,
        DENSE_RANK() OVER w_is_explicit_artist_id_by_rel AS release_date_dense_rank,
        COUNT(*) OVER (PARTITION BY artist_id, is_explicit) AS rows_in_artist_id_is_explicit,
        LAST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id_is_explicit, 0), 4) AS duration_sec_share_of_artist_id_is_explicit,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id) AS rows_in_is_explicit_artist_id,
        ROUND(CUME_DIST() OVER w_release_date_month_by_releas, 3) AS release_date_cume_dist
    FROM cleaned
    WINDOW w_is_explicit_by_bpm AS (PARTITION BY is_explicit ORDER BY bpm DESC, id), w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id), w_is_explicit_artist_id_by_rel AS (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC, id), w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_release_date_month_by_releas AS (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date DESC NULLS FIRST)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        release_date_percent_rank,
        next_release_date,
        is_explicit_artist_id_max_bpm,
        next_release_date_2,
        release_date_dense_rank,
        rows_in_artist_id_is_explicit,
        duration_sec_share_of_artist_id_is_explicit,
        release_date_cume_dist,
        INITCAP(TRIM(title)) AS title_clean,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref,
        QUARTER(next_release_date) AS next_release_date_quarter
    FROM with_windows
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    release_date_percent_rank,
    next_release_date,
    is_explicit_artist_id_max_bpm,
    next_release_date_2,
    rows_in_artist_id_is_explicit,
    release_date_cume_dist,
    title_clean,
    title_ref,
    next_release_date_quarter
FROM annotated
ORDER BY is_explicit_artist_id_max_bpm DESC, release_date_percent_rank DESC NULLS FIRST
