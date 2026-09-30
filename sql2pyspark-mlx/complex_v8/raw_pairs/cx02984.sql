WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label,
        DATE_TRUNC('MONTH', played_at) AS played_at_month
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-03-02'
),
with_parent_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        s.played_at_label,
        s.played_at_month,
        l.interests AS listener_interests,
        l.birth_year AS listener_birth_year,
        t.duration_sec AS track_duration_sec
    FROM filtered_plays s
    JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(played_at_month, 'yyyy-MM-dd HH:00') AS played_at_month_label
    FROM with_parent_listener
),
deduped AS (
    -- Keep the earliest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        played_at_label,
        played_at_month,
        listener_interests,
        listener_birth_year,
        track_duration_sec,
        played_at_month_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY track_duration_sec, id) AS recency_rank
        FROM scored
    ) ranked
    WHERE recency_rank = 1
),
rollup_base AS (
    -- Summarise per group
    SELECT
        track_id,
        source,
        listener_id,
        SUM(track_duration_sec) AS total_track_duration_sec,
        MIN(track_duration_sec) AS min_track_duration_sec
    FROM deduped
    GROUP BY track_id, source, listener_id
    HAVING MAX(track_duration_sec) > 154
)
SELECT
    track_id,
    source,
    listener_id,
    min_track_duration_sec
FROM rollup_base
ORDER BY source, track_id DESC NULLS FIRST, listener_id
LIMIT 10
