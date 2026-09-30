WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        CASE WHEN source IN ('radio', 'playlist') THEN 'group_a' WHEN source = 'album' THEN 'group_b' ELSE 'other' END AS source_group,
        CASE WHEN source IN ('playlist', 'search') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group_2,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE source IN ('album', 'search', 'playlist')
      AND device <> 'car'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        device_slug,
        ROW_NUMBER() OVER w_all_by_played_at AS played_at_seq,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id
    FROM recent_plays
    WINDOW w_all_by_played_at AS (ORDER BY played_at, id)
),
with_parent AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.device_slug,
        s.played_at_seq,
        t.title AS track_title,
        t.release_date AS track_release_date,
        t.is_explicit AS track_is_explicit
    FROM with_windows s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.release_date >= DATE '2024-09-26'
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, played_at_seq, ms_played
    FROM with_parent
) src
UNPIVOT (
    amount FOR metric IN (played_at_seq, ms_played)
) unp
