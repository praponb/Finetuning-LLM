WITH filtered AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref,
        QUARTER(release_date) AS release_date_quarter
    FROM tracks
    WHERE (NOT is_explicit OR SIZE(moods) >= 2)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(release_date_quarter) OVER (PARTITION BY release_date_quarter, is_explicit ORDER BY duration_sec DESC NULLS FIRST, id) AS next_release_date_quarter,
        LAG(bpm) OVER (ORDER BY duration_sec DESC, id) AS prev_bpm,
        RANK() OVER w_all_by_duration_sec_ties AS duration_sec_rank,
        LAST_VALUE(release_date_quarter) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date_quarter,
        ROUND(release_date_quarter - AVG(release_date_quarter) OVER (PARTITION BY artist_id), 2) AS release_date_quarter_vs_artist_id_avg,
        FIRST_VALUE(release_date_quarter) IGNORE NULLS OVER (PARTITION BY release_date_quarter, is_explicit ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date_quarter,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        NTH_VALUE(bpm, 2) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        LEAD(release_date) OVER (PARTITION BY release_date_quarter, is_explicit ORDER BY duration_sec DESC NULLS FIRST, id) AS next_release_date,
        NTH_VALUE(release_date, 2) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        NTH_VALUE(release_date_quarter, 2) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date_quarter,
        ROUND(CUME_DIST() OVER w_release_date_quarter_is_expl, 3) AS duration_sec_cume_dist
    FROM filtered
    WINDOW w_all_by_duration_sec_ties AS (ORDER BY duration_sec DESC), w_release_date_quarter_is_expl AS (PARTITION BY release_date_quarter, is_explicit ORDER BY duration_sec DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    title_ref,
    next_release_date_quarter,
    prev_bpm,
    duration_sec_rank,
    last_release_date_quarter,
    first_known_release_date_quarter,
    rows_in_artist_id,
    second_bpm,
    next_release_date,
    second_release_date,
    second_release_date_quarter,
    duration_sec_cume_dist,
    CONCAT(title_ref, '#', CAST(prev_bpm AS STRING)) AS title_ref_ref
FROM metrics
ORDER BY artist_id DESC, title_ref DESC
