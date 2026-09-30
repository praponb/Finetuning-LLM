WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        COALESCE(bpm, 0) AS bpm_filled
    FROM tracks
    WHERE is_explicit = FALSE
      AND bpm > 100
),
combined_details AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.bpm_filled,
        a.artist_name,
        a.country AS artist_country
    FROM valid_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(release_date) OVER (ORDER BY duration_sec DESC, id) AS prev_release_date,
        ROUND(AVG(bpm) OVER (ORDER BY release_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_6_prior,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_country ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        LEAD(release_date) OVER (ORDER BY release_date, id) AS next_release_date,
        MAX(duration_sec) OVER (PARTITION BY is_explicit, artist_country ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_so_far,
        SUM(duration_sec) OVER (PARTITION BY artist_country, artist_id ORDER BY bpm_filled NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_sum_2_prior,
        SUM(bpm_filled) OVER (PARTITION BY is_explicit, artist_country ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm_filled,
        ROUND(CUME_DIST() OVER w_artist_country_by_bpm, 3) AS bpm_cume_dist,
        MIN(bpm) OVER (PARTITION BY artist_country ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_bpm_so_far,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit,
        FIRST_VALUE(bpm_filled) OVER (PARTITION BY artist_country, artist_id ORDER BY bpm_filled NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm_filled,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date)), 2) AS bpm_vs_is_explicit_release_date_month_avg,
        SIZE(COLLECT_SET(artist_country) OVER (PARTITION BY is_explicit)) AS distinct_artist_country_in_is_explicit,
        ROUND(AVG(bpm) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_7
    FROM combined_details
    WINDOW w_artist_country_by_bpm AS (PARTITION BY artist_country ORDER BY bpm DESC, id)
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    bpm,
    bpm_filled,
    artist_country,
    prev_release_date,
    first_known_release_date,
    next_release_date,
    max_duration_sec_so_far,
    running_bpm_filled,
    bpm_cume_dist,
    min_bpm_so_far,
    rows_in_is_explicit,
    bpm_vs_is_explicit_release_date_month_avg,
    distinct_artist_country_in_is_explicit,
    bpm_avg_7
FROM ranked
ORDER BY prev_release_date NULLS LAST
