WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE source = 'playlist'
)
SELECT id AS key_id, device AS name, ms_played AS qty
FROM filtered_plays
WHERE listener_id >= 17
  AND ms_played < 225733
UNION ALL
SELECT birth_year, country, id
FROM listeners
WHERE SIZE(interests) >= 1
ORDER BY qty DESC
