WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT(device, '#', CAST(id AS STRING)) AS device_ref,
        LENGTH(TRIM(source)) AS source_len,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE ms_played > 56848
      AND source NOT IN ('playlist', 'radio')
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(source_slug), 1, 2) AS source_slug_prefix,
        source_len + ms_played AS source_len_plus_ms_played,
        COALESCE(source_len, 0) AS source_len_filled
    FROM cleaned
),
latest_per_key AS (
    -- Keep the latest row per device, listener_id
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        device_ref,
        source_len,
        source_slug,
        source_slug_prefix,
        source_len_plus_ms_played,
        source_len_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device, listener_id ORDER BY source_len_plus_ms_played DESC, id DESC) AS rn
        FROM annotated
    ) ranked
    WHERE rn = 1
),
with_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.device_ref,
        s.source_slug_prefix,
        s.source_len_plus_ms_played,
        s.source_len_filled,
        l.signup_date AS listener_signup_date,
        l.username AS listener_username
    FROM latest_per_key s
    JOIN listeners l ON l.id = s.listener_id AND UPPER(TRIM(l.username)) LIKE 'G%'
)
-- Final projection
SELECT DISTINCT ms_played, listener_id
FROM with_details_listener
ORDER BY listener_id DESC
