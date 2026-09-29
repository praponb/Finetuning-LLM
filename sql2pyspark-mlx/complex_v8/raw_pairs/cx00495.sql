WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        YEAR(release_date) AS release_date_year
    FROM tracks
    WHERE release_date >= DATE '2024-06-03'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        release_date_year,
        SIZE(COLLECT_SET(artist_id) OVER w_is_explicit) AS distinct_artist_id_in_is_explicit,
        SUM(release_date_year) OVER (PARTITION BY is_explicit) AS is_explicit_sum_release_date_year,
        MIN(release_date_year) OVER (PARTITION BY is_explicit) AS is_explicit_min_release_date_year,
        COUNT(*) OVER (PARTITION BY artist_id, is_explicit) AS rows_in_artist_id_is_explicit,
        LEAD(release_date_year, 2) OVER (ORDER BY release_date ASC, id) AS next2_release_date_year,
        ROUND(release_date_year / NULLIF(SUM(release_date_year) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS release_date_year_share_of_artist_id_is_explicit,
        ROW_NUMBER() OVER (ORDER BY release_date_year DESC NULLS FIRST, id) AS release_date_year_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY release_date_year, is_explicit ORDER BY release_date_year ASC), 3) AS release_date_year_cume_dist
    FROM filtered
    WINDOW w_is_explicit AS (PARTITION BY is_explicit)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE release_date_year_seq = 1
),
totals AS (
    -- Summarise per group
    SELECT
        release_date_year,
        is_explicit,
        next2_release_date_year,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        MAX(release_date) AS last_release_date,
        SUM(is_explicit_sum_release_date_year) AS total_is_explicit_sum_release_date_year
    FROM top_rows
    WHERE is_explicit_min_release_date_year > 9318
    GROUP BY 1, 2, 3
),
with_windows_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY is_explicit, release_date_year) AS rows_in_is_explicit_release_date_year,
        MIN(release_date_year) OVER (PARTITION BY is_explicit, release_date_year) AS is_explicit_release_date_year_min_release_date_y
    FROM totals
)
SELECT
    release_date_year,
    is_explicit,
    title_list,
    last_release_date,
    total_is_explicit_sum_release_date_year
FROM with_windows_2
WHERE next2_release_date_year < 2025
ORDER BY release_date_year DESC NULLS FIRST, last_release_date NULLS LAST
