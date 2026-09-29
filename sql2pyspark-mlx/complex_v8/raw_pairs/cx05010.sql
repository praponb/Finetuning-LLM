WITH scoped AS (
    -- Filter tracks
    SELECT *
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'party')
      AND duration_sec > 165
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        title,
        release_date,
        is_explicit,
        bpm,
        ABS(duration_sec - 209) AS duration_sec_distance,
        GREATEST(bpm, duration_sec) AS greatest_bpm_duration_sec,
        ABS(duration_sec - 352) AS duration_sec_distance_2
    FROM scoped
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count_2,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.id) AS first_played_at
    FROM scored s
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        title,
        release_date,
        is_explicit,
        duration_sec_distance,
        greatest_bpm_duration_sec,
        duration_sec_distance_2,
        play_count,
        play_count_2,
        first_played_at,
        duration_sec_distance_2 - LAG(duration_sec_distance_2) OVER (PARTITION BY play_count, is_explicit ORDER BY play_count_2, id) AS duration_sec_distance_2_change,
        ROUND(AVG(greatest_bpm_duration_sec) OVER (PARTITION BY YEAR(release_date)), 2) AS release_date_year_avg_greatest_bpm_duration_sec,
        RANK() OVER w_play_count_is_explicit_by_pl AS play_count_2_rank,
        SUM(greatest_bpm_duration_sec) OVER (PARTITION BY play_count, is_explicit ORDER BY play_count_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_greatest_bpm_duration_sec,
        LAST_VALUE(first_played_at) IGNORE NULLS OVER (PARTITION BY play_count, is_explicit ORDER BY play_count_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_played_at,
        FIRST_VALUE(greatest_bpm_duration_sec) OVER (PARTITION BY play_count, is_explicit ORDER BY play_count_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_greatest_bpm_duration_sec
    FROM with_stats
    WINDOW w_play_count_is_explicit_by_pl AS (PARTITION BY play_count, is_explicit ORDER BY play_count_2, id)
)
-- Subtotals with GROUPING SETS
SELECT
    is_explicit,
    play_count_2,
    GROUPING_ID() AS grouping_level,
    SUM(play_count_2) AS total_play_count_2
FROM with_windows
GROUP BY GROUPING SETS ((is_explicit, play_count_2), (is_explicit), ())
ORDER BY is_explicit NULLS LAST, play_count_2 NULLS LAST
