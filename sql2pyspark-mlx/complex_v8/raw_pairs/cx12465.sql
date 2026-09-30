WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        YEAR(played_at) AS played_at_year,
        UPPER(TRIM(source)) AS source_clean
    FROM plays
    WHERE source = 'playlist'
),
enriched_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.played_at_year,
        s.source_clean,
        l.interests AS listener_interests,
        l.username AS listener_username,
        l.signup_date AS listener_signup_date
    FROM recent s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) FILTER (WHERE is_skipped = TRUE) AS play_is_skipped_count,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count,
        COALESCE(p.play_count, 0) AS play_count
    FROM enriched_listener s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
)
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    device,
    is_skipped,
    source,
    source_clean,
    listener_interests,
    listener_username,
    play_is_skipped_count,
    play_count,
    DATEDIFF(DATE '2026-01-01', listener_signup_date) AS days_since_listener_signup_date
FROM summarized_rows
ORDER BY id DESC NULLS FIRST
