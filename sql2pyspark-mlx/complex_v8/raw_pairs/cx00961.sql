WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        UPPER(TRIM(source)) AS source_clean
    FROM plays
    WHERE ms_played BETWEEN 26344 AND 290754
      AND is_skipped = TRUE
)
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    source,
    SUBSTRING(TRIM(source), 1, 2) AS source_prefix,
    LENGTH(TRIM(source)) AS source_len
FROM scoped_plays
WHERE listener_id IS NOT NULL
ORDER BY id DESC, source DESC
