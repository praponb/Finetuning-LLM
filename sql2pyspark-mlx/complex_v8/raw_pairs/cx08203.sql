WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-24 09:00:00'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(days_since_played_at, 0) AS days_since_played_at_filled,
        TO_DATE(played_at) AS played_at_date
    FROM recent_plays
    WHERE device <> 'tv'
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.source IS NOT NULL
              AND p.listener_id IS NOT NULL
        )
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(days_since_played_at) OVER (PARTITION BY track_id ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_played_at,
        MAX(days_since_played_at) OVER (PARTITION BY track_id ORDER BY played_at ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS days_since_played_at_max_1_prior,
        COUNT(*) OVER (PARTITION BY device, TO_DATE(played_at)) AS rows_in_device_played_at_to_date,
        NTILE(10) OVER (PARTITION BY track_id ORDER BY played_at ASC, id) AS played_at_decile,
        ROUND(days_since_played_at - AVG(days_since_played_at) OVER w_device_played_at_to_date, 2) AS days_since_played_at_vs_device_played_at_to_date
    FROM without_matches
    WINDOW w_device_played_at_to_date AS (PARTITION BY device, TO_DATE(played_at))
)
SELECT
    id,
    listener_id,
    device,
    is_skipped_label,
    days_since_played_at,
    days_since_played_at_filled,
    played_at_date,
    days_since_played_at_max_1_prior,
    played_at_decile,
    days_since_played_at_vs_device_played_at_to_date,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label_2
FROM ranked
WHERE days_since_played_at <= 385
