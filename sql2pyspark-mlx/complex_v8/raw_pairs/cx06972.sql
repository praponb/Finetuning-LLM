WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE is_explicit = TRUE
      AND bpm < 147
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit ORDER BY bpm, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_7,
        LAST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_artist_id_is_explicit, 0), 4) AS bpm_share_of_artist_id_is_explicit,
        ROUND(AVG(bpm) OVER (ORDER BY release_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_7_2,
        COUNT(*) OVER (PARTITION BY is_explicit ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(4) OVER (PARTITION BY is_explicit ORDER BY bpm, id) AS bpm_quartile,
        FIRST_VALUE(bpm) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        ROW_NUMBER() OVER w_artist_id_by_bpm AS rn,
        COUNT(*) OVER (PARTITION BY artist_id, is_explicit) AS rows_in_artist_id_is_explicit,
        MAX(bpm) OVER (PARTITION BY artist_id, is_explicit) AS artist_id_is_explicit_max_bpm,
        ROUND(CUME_DIST() OVER w_artist_id_by_bpm_ties, 3) AS bpm_cume_dist
    FROM cleaned_tracks
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_artist_id_by_bpm AS (PARTITION BY artist_id ORDER BY bpm, id), w_artist_id_by_bpm_ties AS (PARTITION BY artist_id ORDER BY bpm)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        release_date,
        bpm,
        bpm_avg_7,
        bpm_avg_7_2,
        bpm_quartile,
        first_bpm,
        rn,
        artist_id_is_explicit_max_bpm,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY running_count ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        LEAD(release_date) OVER (PARTITION BY is_explicit ORDER BY bpm_cume_dist DESC, id) AS next_release_date,
        LAST_VALUE(running_count) OVER (PARTITION BY is_explicit ORDER BY bpm_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_running_count,
        LAG(bpm_avg_7, 3) OVER w_is_explicit_by_bpm_cume_dist AS prev3_bpm_avg_7,
        ROUND(AVG(bpm_avg_7) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)), 2) AS release_date_month_avg_bpm_avg_7,
        FIRST_VALUE(bpm_quartile) OVER (ORDER BY running_count ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm_quartile
    FROM metrics
    WINDOW w_is_explicit_by_bpm_cume_dist AS (PARTITION BY is_explicit ORDER BY bpm_cume_dist DESC, id)
)
-- Final projection
SELECT
    id,
    release_date,
    bpm,
    bpm_avg_7,
    bpm_avg_7_2,
    bpm_quartile,
    first_bpm,
    artist_id_is_explicit_max_bpm,
    first_known_bpm,
    next_release_date,
    prev3_bpm_avg_7,
    release_date_month_avg_bpm_avg_7,
    first_bpm_quartile - bpm_avg_7_2 AS first_bpm_quartile_minus_bpm_avg_7_2
FROM ranked
