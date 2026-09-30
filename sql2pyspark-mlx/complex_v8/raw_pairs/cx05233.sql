WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE is_skipped = FALSE
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(ms_played - AVG(ms_played) OVER w_source, 2) AS ms_played_vs_source_avg,
        MAX(ms_played) OVER (PARTITION BY listener_id ORDER BY played_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_max_7,
        SUM(ms_played) OVER (PARTITION BY listener_id ORDER BY played_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ms_played_sum_4
    FROM valid_plays
    WINDOW w_source AS (PARTITION BY source)
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        ms_played_max_7,
        UPPER(TRIM(source)) AS source_clean,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label
    FROM ranked
    WHERE ms_played_vs_source_avg >= -240000.0
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at
    FROM labeled s
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(ms_played_max_7) OVER (PARTITION BY source, device ORDER BY played_at DESC, id) AS next_ms_played_max_7,
        SUM(ms_played_max_7) OVER w_last_played_at_month AS last_played_at_month_sum_ms_played_max_7,
        SUM(ms_played) OVER (PARTITION BY source, device ORDER BY played_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ms_played_sum_3_prior,
        COUNT(*) OVER (PARTITION BY MONTH(last_played_at)) AS rows_in_last_played_at_month,
        LAG(ms_played) OVER w_is_skipped_track_id_by_ms_pl AS prev_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY MONTH(last_played_at)), 0), 4) AS ms_played_share_of_last_played_at_month
    FROM profiled
    WINDOW w_last_played_at_month AS (PARTITION BY MONTH(last_played_at)), w_is_skipped_track_id_by_ms_pl AS (PARTITION BY is_skipped, track_id ORDER BY ms_played_max_7 DESC, id)
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.source_clean,
        s.played_at_label,
        s.first_played_at,
        s.next_ms_played_max_7,
        s.last_played_at_month_sum_ms_played_max_7,
        s.ms_played_sum_3_prior,
        s.prev_ms_played,
        s.ms_played_share_of_last_played_at_month,
        p.id AS play_id,
        p.device AS play_device,
        p.ms_played AS play_ms_played,
        p.listener_id AS play_listener_id,
        p.is_skipped AS play_is_skipped
    FROM with_windows s
    LEFT JOIN plays p ON p.track_id = s.track_id AND p.device IN ('desktop', 'phone')
),
joined_track AS (
    -- Enrich with dimension data
    SELECT s.*, t.is_explicit AS track_is_explicit
    FROM with_children_plays s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.is_explicit = TRUE
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    source,
    played_at_label,
    first_played_at,
    next_ms_played_max_7,
    last_played_at_month_sum_ms_played_max_7,
    ms_played_sum_3_prior,
    ms_played_share_of_last_played_at_month,
    play_id,
    play_device,
    play_listener_id,
    play_is_skipped,
    track_is_explicit,
    LEAST(ms_played, next_ms_played_max_7) AS least_ms_played_next_ms_played_max_7
FROM joined_track
ORDER BY play_id DESC, least_ms_played_next_ms_played_max_7 DESC
