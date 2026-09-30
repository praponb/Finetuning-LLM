WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE source <> 'album'
      AND listener_id >= 18
)
SELECT
    'play' AS src,
    listener_id AS qty,
    id AS entity_id,
    played_at
FROM plays
UNION ALL
SELECT
    'play_2' AS src,
    track_id AS qty,
    ms_played AS entity_id,
    played_at
FROM plays
