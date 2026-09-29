WITH valid AS (
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
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
    FROM plays
    WHERE HOUR(played_at) BETWEEN 6 AND 8
),
with_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source,
        s.device_prefix,
        s.played_at_label,
        l.birth_year AS listener_birth_year,
        t.release_date AS track_release_date
    FROM valid s
    LEFT JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        played_at,
        source,
        device_prefix,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id,
        SUM(listener_birth_year) OVER w_listener_id AS listener_id_sum_listener_birth_year,
        ROW_NUMBER() OVER w_all_by_track_release_date AS seq_no
    FROM with_details
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_all_by_track_release_date AS (ORDER BY track_release_date ASC, id)
)
SELECT
    id,
    track_id,
    played_at,
    source,
    device_prefix,
    rows_in_listener_id,
    listener_id_sum_listener_birth_year,
    seq_no,
    LENGTH(TRIM(device_prefix)) AS device_prefix_len,
    CASE WHEN source IN ('playlist', 'radio') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group
FROM sequenced
ORDER BY rows_in_listener_id, played_at, id
LIMIT 10
