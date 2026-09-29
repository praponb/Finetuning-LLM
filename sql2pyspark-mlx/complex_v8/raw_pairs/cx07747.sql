WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE NOT is_explicit
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        title_slug,
        LAST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        LAG(release_date, 3) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm DESC, id) AS prev3_release_date,
        COUNT(*) OVER w_is_explicit_artist_id AS rows_in_is_explicit_artist_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY bpm DESC), 3) AS bpm_percent_rank,
        COUNT(*) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(release_date) OVER (ORDER BY bpm DESC, id) AS prev_release_date,
        MIN(bpm) OVER w_is_explicit_artist_id AS is_explicit_artist_id_min_bpm,
        ROUND(PERCENT_RANK() OVER (ORDER BY release_date ASC NULLS LAST), 3) AS release_date_percent_rank,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit, artist_id), 0), 4) AS bpm_share_of_is_explicit_artist_id,
        MAX(bpm) OVER (ORDER BY release_date ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS bpm_max_3,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC), 3) AS release_date_cume_dist
    FROM cleaned
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        title_slug,
        prev3_release_date,
        rows_in_is_explicit_artist_id,
        bpm_percent_rank,
        running_count,
        is_explicit_artist_id_min_bpm,
        release_date_percent_rank,
        bpm_share_of_is_explicit_artist_id,
        bpm_max_3,
        release_date_cume_dist,
        DATE_ADD(prev_release_date, 365) AS prev_release_date_plus_365d,
        DATE_FORMAT(prev_release_date, 'yyyy-MM') AS prev_release_date_label
    FROM ranked
)
SELECT
    id,
    artist_id,
    is_explicit,
    bpm,
    title_slug,
    rows_in_is_explicit_artist_id,
    bpm_percent_rank,
    running_count,
    is_explicit_artist_id_min_bpm,
    bpm_share_of_is_explicit_artist_id,
    bpm_max_3,
    release_date_cume_dist,
    prev_release_date_label,
    DATEDIFF(DATE '2025-06-30', prev3_release_date) AS days_since_prev3_release_date
FROM tagged
ORDER BY release_date_cume_dist DESC, is_explicit_artist_id_min_bpm
