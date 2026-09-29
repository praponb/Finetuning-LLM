WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        UPPER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE artist_id >= 6
      AND SIZE(moods) >= 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        title_clean,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY YEAR(release_date), is_explicit), 2) AS duration_sec_vs_release_date_year_is_explicit_av,
        ROW_NUMBER() OVER w_is_explicit_artist_id_by_rel_2 AS release_date_seq,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        MIN(duration_sec) OVER (PARTITION BY YEAR(release_date), is_explicit) AS release_date_year_is_explicit_min_duration_sec,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id, 0), 4) AS duration_sec_share_of_artist_id,
        MIN(duration_sec) OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_so_far,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date, id) AS release_date_seq_2,
        RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC NULLS LAST) AS release_date_rank,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC NULLS LAST), 3) AS release_date_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec), 3) AS duration_sec_percent_rank,
        MAX(duration_sec) OVER w_artist_id AS artist_id_max_duration_sec,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC, id) AS duration_sec_change
    FROM base
    WINDOW w_is_explicit_artist_id_by_rel_2 AS (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC, id), w_artist_id AS (PARTITION BY artist_id)
)
SELECT
    id,
    duration_sec,
    is_explicit,
    second_duration_sec,
    duration_sec_vs_release_date_year_is_explicit_av,
    release_date_seq,
    first_known_release_date,
    release_date_year_is_explicit_min_duration_sec,
    duration_sec_share_of_artist_id,
    min_duration_sec_so_far,
    release_date_rank,
    running_count,
    release_date_cume_dist,
    duration_sec_percent_rank,
    duration_sec_change
FROM ranked
ORDER BY min_duration_sec_so_far
