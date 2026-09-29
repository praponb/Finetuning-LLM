WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'P%'
      AND NOT is_explicit
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(duration_sec) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_so_far,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        RANK() OVER w_is_explicit_by_release_date_2 AS release_date_rank,
        NTILE(4) OVER (ORDER BY bpm, id) AS bpm_quartile,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS bpm_share_of_artist_id_is_explicit,
        ROUND(PERCENT_RANK() OVER w_all_by_release_date, 3) AS release_date_percent_rank,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_bpm,
        LAG(bpm, 1, 0) OVER (PARTITION BY is_explicit ORDER BY release_date ASC, id) AS prev_bpm
    FROM base
    WINDOW w_is_explicit_by_release_date_2 AS (PARTITION BY is_explicit ORDER BY release_date ASC), w_all_by_release_date AS (ORDER BY release_date ASC, id)
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        title_prefix,
        max_duration_sec_so_far,
        bpm_share_of_artist_id,
        first_known_release_date,
        bpm_quartile,
        bpm_vs_artist_id_avg,
        bpm_share_of_artist_id_is_explicit,
        release_date_percent_rank,
        artist_id_avg_bpm,
        prev_bpm
    FROM ranked
    WHERE release_date_rank <= 2
      AND ARRAY_CONTAINS(moods, 'party')
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    title_prefix,
    bpm_share_of_artist_id,
    first_known_release_date,
    bpm_vs_artist_id_avg,
    release_date_percent_rank,
    artist_id_avg_bpm,
    prev_bpm,
    IF(is_explicit, 'yes', 'no') AS is_explicit_label
FROM top_rows
ORDER BY is_explicit_label
