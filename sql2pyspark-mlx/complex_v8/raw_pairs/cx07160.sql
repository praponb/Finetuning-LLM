WITH prepared_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
    FROM plays
    WHERE ms_played > 160264
      AND is_skipped = FALSE
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM prepared_plays s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
        )
      AND s.ms_played <= 301149
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at,
        UPPER(TRIM(device)) AS device_clean
    FROM with_activity
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        days_since_played_at,
        FIRST_VALUE(played_at) OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        ROW_NUMBER() OVER w_all_by_ms_played AS seq_no,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY TO_DATE(played_at)), 0), 4) AS ms_played_share_of_played_at_to_date
    FROM derived
    WINDOW w_all_by_ms_played AS (ORDER BY ms_played ASC, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE seq_no <= 5
),
filtered AS (
    -- Keep rows whose track matches
    SELECT s.*
    FROM top_per_group s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.id = s.track_id
              AND t.bpm > 76
        )
)
-- Final projection
SELECT
    id,
    track_id,
    played_at,
    is_skipped,
    days_since_played_at,
    seq_no,
    ROUND(ms_played / NULLIF(days_since_played_at, 0), 3) AS ms_played_per_days_since_played_at,
    LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
FROM filtered
ORDER BY track_id, days_since_played_at
