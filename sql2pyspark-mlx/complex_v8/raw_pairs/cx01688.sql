WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix,
        DAYOFWEEK(played_at) AS played_at_dayofweek
    FROM plays
    WHERE (listener_id IS NOT NULL OR source = 'search')
),
enriched_track AS (
    -- Enrich with dimension data
    SELECT s.*, t.moods AS track_moods
    FROM filtered_plays s
    JOIN tracks t ON t.id = s.track_id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_ym,
        listener_id,
        device,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE source IN ('playlist', 'radio', 'album')) AS n_source_in_playlist_radio_alb
    FROM enriched_track
    WHERE ARRAY_CONTAINS(track_moods, 'party')
    GROUP BY DATE_FORMAT(played_at, 'yyyy-MM'), listener_id, device
    HAVING COUNT(*) >= 2
),
with_details_listener AS (
    -- Attach listener attributes
    SELECT
        s.*,
        l.country AS listener_country,
        l.username AS listener_username,
        l.signup_date AS listener_signup_date
    FROM stats s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
latest AS (
    -- Keep the latest row per listener_country
    SELECT
        played_at_ym,
        listener_id,
        device,
        row_count,
        n_source_in_playlist_radio_alb,
        listener_country,
        listener_username,
        listener_signup_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_country ORDER BY row_count DESC, played_at_ym DESC, listener_id DESC, device DESC) AS recency_rank
        FROM with_details_listener
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    played_at_ym,
    row_count,
    n_source_in_playlist_radio_alb,
    listener_country,
    listener_username,
    COALESCE(device, 'UNKNOWN') AS device_or_unknown,
    FLOOR(row_count / 2) * 2 AS row_count_bucket
FROM latest
ORDER BY device_or_unknown DESC NULLS FIRST
