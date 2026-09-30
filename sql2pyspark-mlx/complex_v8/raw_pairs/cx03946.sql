WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm
    FROM tracks
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        ROUND(CUME_DIST() OVER w_is_explicit_by_release_date, 3) AS release_date_cume_dist,
        MIN(bpm) OVER (PARTITION BY artist_id ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_bpm_so_far,
        ROUND(AVG(bpm) OVER w_is_explicit, 2) AS is_explicit_avg_bpm,
        SIZE(COLLECT_SET(artist_id) OVER w_is_explicit) AS distinct_artist_id_in_is_explicit,
        LAG(release_date) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id) AS prev_release_date,
        LAG(bpm, 1, 0) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC NULLS LAST, id) AS prev_bpm,
        DENSE_RANK() OVER (PARTITION BY is_explicit ORDER BY release_date) AS release_date_dense_rank,
        RANK() OVER (PARTITION BY is_explicit ORDER BY release_date) AS release_date_rank,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY is_explicit), 2) AS bpm_vs_is_explicit_avg,
        ROW_NUMBER() OVER (PARTITION BY is_explicit ORDER BY release_date, id) AS rn,
        LAST_VALUE(release_date) OVER (PARTITION BY is_explicit ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date,
        COUNT(*) OVER w_is_explicit AS rows_in_is_explicit
    FROM cleaned_tracks
    WINDOW w_is_explicit_by_release_date AS (PARTITION BY is_explicit ORDER BY release_date, id), w_is_explicit AS (PARTITION BY is_explicit)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE release_date_rank <= 5
)
SELECT
    id,
    release_date,
    is_explicit,
    bpm,
    release_date_cume_dist,
    is_explicit_avg_bpm,
    distinct_artist_id_in_is_explicit,
    prev_bpm,
    release_date_dense_rank,
    release_date_rank,
    last_release_date,
    rows_in_is_explicit
FROM first_rows
ORDER BY release_date_rank DESC, last_release_date
