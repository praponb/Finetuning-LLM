WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE device = 'car'
      AND is_skipped = TRUE
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM filtered_plays s
    JOIN plays p ON p.track_id = s.track_id
),
labeled AS (
    -- Add calculated columns
    SELECT *, MONTH(play_played_at) AS play_played_at_month
    FROM with_children
),
with_parent_play AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.play_id,
        s.play_is_skipped,
        s.play_ms_played,
        s.play_played_at,
        s.play_device,
        s.play_played_at_month,
        p.ms_played AS play_ms_played_2,
        p.track_id AS play_track_id,
        l.country AS listener_country,
        l.birth_year AS listener_birth_year
    FROM labeled s
    JOIN plays p ON p.id = s.play_id
    LEFT JOIN listeners l ON l.id = p.listener_id
    WHERE p.track_id >= 5
),
latest AS (
    -- Keep the latest row per track_id, play_track_id
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        play_id,
        play_is_skipped,
        play_ms_played,
        play_played_at,
        play_device,
        play_played_at_month,
        play_ms_played_2,
        play_track_id,
        listener_country,
        listener_birth_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id, play_track_id ORDER BY play_played_at_month DESC, id DESC, play_id DESC) AS rn
        FROM with_parent_play
    ) ranked
    WHERE rn = 1
),
enriched_play AS (
    -- Attach play attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.play_id,
        s.play_is_skipped,
        s.play_ms_played,
        s.play_played_at,
        s.play_device,
        s.play_ms_played_2,
        s.play_track_id,
        s.listener_birth_year,
        p.played_at AS play_played_at_2
    FROM latest s
    LEFT JOIN plays p ON p.id = s.play_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE p.is_skipped = FALSE) FROM plays p WHERE p.track_id = s.play_track_id) AS play_is_skipped_count
    FROM enriched_play s
)
SELECT id, play_id, metric, metric_value
FROM (
    SELECT id, play_id, listener_birth_year, play_ms_played, play_is_skipped_count
    FROM summarized_rows
) src
UNPIVOT (
    metric_value FOR metric IN (listener_birth_year, play_ms_played, play_is_skipped_count)
) unp
