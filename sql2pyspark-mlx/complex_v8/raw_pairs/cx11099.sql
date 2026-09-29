WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE (artist_id IS NOT NULL OR is_explicit = FALSE)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(duration_sec) OVER w_is_explicit AS is_explicit_max_duration_sec,
        SUM(bpm) OVER (PARTITION BY is_explicit) AS is_explicit_sum_bpm,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit)) AS distinct_artist_id_in_is_explicit,
        bpm - LAG(bpm) OVER w_artist_id_is_explicit_by_bpm AS bpm_change,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_2_prior,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec DESC), 3) AS duration_sec_cume_dist,
        ROUND(CUME_DIST() OVER w_artist_id_is_explicit_by_bpm_3, 3) AS bpm_cume_dist,
        ROUND(CUME_DIST() OVER w_artist_id_by_duration_sec, 3) AS duration_sec_cume_dist_2,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit), 0), 4) AS bpm_share_of_is_explicit
    FROM filtered
    WINDOW w_is_explicit AS (PARTITION BY is_explicit), w_artist_id_is_explicit_by_bpm AS (PARTITION BY artist_id, is_explicit ORDER BY bpm ASC NULLS LAST, id), w_artist_id_is_explicit_by_bpm_3 AS (PARTITION BY artist_id, is_explicit ORDER BY bpm ASC NULLS LAST), w_artist_id_by_duration_sec AS (PARTITION BY artist_id ORDER BY duration_sec, id)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        is_explicit,
        distinct_artist_id_in_is_explicit,
        SUM(bpm) FILTER (WHERE is_explicit = TRUE) AS bpm_is_explicit_true,
        ROUND(SUM(bpm_cume_dist) / NULLIF(SUM(is_explicit_max_duration_sec), 0), 3) AS bpm_cume_dist_to_is_explicit_max_duration_sec_ra,
        MIN(release_date) AS first_release_date
    FROM windowed
    GROUP BY is_explicit, distinct_artist_id_in_is_explicit
    HAVING SUM(duration_sec_cume_dist) > 0.3
)
SELECT
    is_explicit,
    distinct_artist_id_in_is_explicit,
    bpm_is_explicit_true,
    bpm_cume_dist_to_is_explicit_max_duration_sec_ra,
    first_release_date,
    MONTH(first_release_date) AS first_release_date_month,
    DATE_FORMAT(first_release_date, 'yyyy-MM') AS first_release_date_label
FROM rollup_base
WHERE first_release_date BETWEEN DATE '2024-06-03' AND DATE '2025-03-29'
ORDER BY bpm_cume_dist_to_is_explicit_max_duration_sec_ra NULLS LAST, is_explicit, distinct_artist_id_in_is_explicit
LIMIT 20
