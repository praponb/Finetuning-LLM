WITH scoped AS (
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
        LENGTH(TRIM(device)) AS device_len,
        CONCAT(device, '#', CAST(ms_played AS STRING)) AS device_ref
    FROM plays
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
        )
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        device_len - LAG(device_len) OVER w_source_by_played_at AS device_len_change,
        ROUND(CUME_DIST() OVER w_all_by_played_at_ties, 3) AS played_at_cume_dist
    FROM screened
    WINDOW w_source_by_played_at AS (PARTITION BY source ORDER BY played_at DESC, id), w_all_by_played_at_ties AS (ORDER BY played_at ASC)
)
SELECT *
FROM (
    SELECT is_skipped, device_len, device, device_len_change
    FROM metrics
) src
PIVOT (
    AVG(device_len_change)
    FOR device IN ('desktop', 'car', 'phone', 'tv')
)
