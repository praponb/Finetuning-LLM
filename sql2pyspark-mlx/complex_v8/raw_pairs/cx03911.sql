WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        COALESCE(bpm, 0) AS bpm_filled,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE release_date < DATE '2025-08-07'
      AND artist_id IS NOT NULL
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(bpm_filled) OVER (PARTITION BY is_explicit, title_len ORDER BY bpm_filled DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS bpm_filled_avg_2,
        SUM(bpm) OVER w_is_explicit AS is_explicit_sum_bpm,
        SUM(bpm) OVER (ORDER BY bpm ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit, title_len ORDER BY bpm_filled DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_2_prior,
        RANK() OVER (PARTITION BY is_explicit, title_len ORDER BY bpm_filled DESC) AS bpm_filled_rank,
        ROUND(title_len - AVG(title_len) OVER (PARTITION BY is_explicit), 2) AS title_len_vs_is_explicit_avg
    FROM valid_tracks
    WINDOW w_is_explicit AS (PARTITION BY is_explicit)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(running_bpm, 0) AS running_bpm_filled,
        running_bpm - duration_sec AS running_bpm_minus_duration_sec
    FROM with_windows
    WHERE title_len_vs_is_explicit_avg < 5.6
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm_filled,
        s.title_len,
        s.bpm_filled_avg_2,
        s.is_explicit_sum_bpm,
        s.running_bpm,
        s.bpm_filled_rank,
        s.running_bpm_minus_duration_sec,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source,
        p.played_at AS play_played_at
    FROM labeled s
    JOIN plays p ON p.track_id = s.id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        bpm_filled,
        title_len,
        is_explicit_sum_bpm,
        running_bpm,
        running_bpm_minus_duration_sec,
        play_id,
        play_source,
        play_played_at,
        SUM(title_len) OVER (ORDER BY is_explicit_sum_bpm, id, play_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS title_len_sum_2_prior,
        RANK() OVER w_all_by_is_explicit_sum_bpm AS is_explicit_sum_bpm_rank,
        COUNT(*) OVER (PARTITION BY play_source, play_id) AS rows_in_play_source_play_id,
        RANK() OVER w_all_by_is_explicit_sum_bpm_t AS is_explicit_sum_bpm_rank_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY is_explicit_sum_bpm), 3) AS is_explicit_sum_bpm_percent_rank
    FROM line_level_plays
    WINDOW w_all_by_is_explicit_sum_bpm AS (ORDER BY is_explicit_sum_bpm, id, play_id), w_all_by_is_explicit_sum_bpm_t AS (ORDER BY is_explicit_sum_bpm)
),
with_parent_play AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.title_len,
        s.is_explicit_sum_bpm,
        s.running_bpm,
        s.running_bpm_minus_duration_sec,
        s.play_id,
        s.play_source,
        s.play_played_at,
        s.title_len_sum_2_prior,
        s.is_explicit_sum_bpm_rank,
        p.played_at AS play_played_at_2
    FROM with_trends s
    JOIN plays p ON p.id = s.play_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT play_id, play_played_at, play_source, running_bpm
    FROM with_parent_play
) src
PIVOT (
    SUM(running_bpm)
    FOR play_source IN ('playlist', 'album', 'radio', 'search')
)
