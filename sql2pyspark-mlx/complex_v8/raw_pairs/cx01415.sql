WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM tracks
    WHERE LOWER(title) LIKE '%bu%'
      AND YEAR(release_date) = 2025
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) FILTER (WHERE source = 'playlist') AS play_source_count
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count
    FROM valid s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY play_source_count ORDER BY duration_sec_per_bpm DESC, id)) AS days_since_prev_release_date,
        NTILE(4) OVER w_play_source_count_by_duratio AS duration_sec_per_bpm_quartile,
        COUNT(*) OVER (PARTITION BY play_source_count, MONTH(release_date)) AS rows_in_play_source_count_release_date_month,
        ROUND(play_source_count / NULLIF(SUM(play_source_count) OVER (PARTITION BY play_source_count, MONTH(release_date)), 0), 4) AS play_source_count_share_of_play_source_count_rel,
        duration_sec_per_bpm - LAG(duration_sec_per_bpm) OVER (PARTITION BY play_source_count ORDER BY duration_sec_per_bpm DESC, id) AS duration_sec_per_bpm_change
    FROM summarized_rows
    WINDOW w_play_source_count_by_duratio AS (PARTITION BY play_source_count ORDER BY duration_sec_per_bpm DESC, id)
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.duration_sec_per_bpm,
        s.title_prefix,
        s.duration_sec_per_bpm_quartile,
        s.rows_in_play_source_count_release_date_month,
        s.play_source_count_share_of_play_source_count_rel,
        s.duration_sec_per_bpm_change,
        p.id AS play_id,
        p.device AS play_device,
        p.source AS play_source,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played
    FROM with_windows s
    LEFT JOIN plays p ON p.track_id = s.id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        title_prefix,
        duration_sec_per_bpm_quartile,
        rows_in_play_source_count_release_date_month,
        duration_sec_per_bpm_change,
        play_id,
        play_source,
        play_ms_played,
        RANK() OVER w_play_id_by_play_ms_played_ti AS play_ms_played_rank,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, play_device ORDER BY play_played_at, id, play_id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY is_explicit, play_device ORDER BY play_played_at) AS play_played_at_dense_rank,
        LEAD(duration_sec_per_bpm, 2) OVER (PARTITION BY play_id ORDER BY play_ms_played DESC NULLS FIRST, id, play_id) AS next2_duration_sec_per_bpm,
        ROUND(PERCENT_RANK() OVER w_is_explicit_play_device_by_p, 3) AS play_played_at_percent_rank
    FROM line_level_plays
    WINDOW w_play_id_by_play_ms_played_ti AS (PARTITION BY play_id ORDER BY play_ms_played DESC NULLS FIRST), w_is_explicit_play_device_by_p AS (PARTITION BY is_explicit, play_device ORDER BY play_played_at, id, play_id)
),
with_parent_play AS (
    -- Enrich with dimension data
    SELECT s.*, p.listener_id AS play_listener_id
    FROM with_trends s
    JOIN plays p ON p.id = s.play_id AND NOT p.is_skipped
)
-- Subtotals with CUBE
SELECT
    play_id,
    play_source,
    play_ms_played_rank,
    MIN(next2_duration_sec_per_bpm) AS min_next2_duration_sec_per_bpm,
    COUNT(*) FILTER (WHERE play_source IN ('search', 'radio', 'playlist')) AS n_play_source_in_search_radio,
    COUNT(*) AS row_count
FROM with_parent_play
GROUP BY CUBE(play_id, play_source, play_ms_played_rank)
ORDER BY play_id NULLS LAST, play_source NULLS LAST, play_ms_played_rank NULLS LAST
