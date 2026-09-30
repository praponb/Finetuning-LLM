WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(source), 1, 2) AS source_prefix
    FROM plays
    WHERE listener_id IS NOT NULL
      AND is_skipped = TRUE
),
joined AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        l.country AS listener_country
    FROM base s
    JOIN listeners l ON l.id = s.listener_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        LENGTH(TRIM(device)) AS device_len,
        COALESCE(listener_country, 'UNKNOWN') AS listener_country_or_unknown
    FROM joined
    WHERE listener_country <> 'BR'
)
SELECT
    id,
    device,
    is_skipped,
    listener_country,
    device_slug,
    device_len,
    listener_country_or_unknown
FROM tagged
ORDER BY id NULLS LAST, device
LIMIT 5
