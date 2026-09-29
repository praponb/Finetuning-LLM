SELECT
    'play' AS record_type,
    device,
    listener_id AS key_id,
    played_at AS happened_at
FROM plays
WHERE device = 'desktop'
UNION ALL
SELECT
    'play_2' AS record_type,
    source AS device,
    ms_played AS key_id,
    played_at AS happened_at
FROM plays
WHERE is_skipped = TRUE
