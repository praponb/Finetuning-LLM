WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix
    FROM plays
    WHERE (source IS NOT NULL OR listener_id <= 9)
),
device_stats AS (
    -- Max ms_played_bucket per device
    SELECT device, MAX(ms_played_bucket) AS max_ms_played_bucket
    FROM base_plays
    GROUP BY device
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ms_played_bucket,
        ROUND(s.ms_played_bucket - g.max_ms_played_bucket, 2) AS ms_played_bucket_minus_max
    FROM base_plays s
    JOIN device_stats g ON g.device = s.device
    WHERE s.ms_played_bucket = g.max_ms_played_bucket
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM above_average s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
        )
      AND s.device = 'car'
),
latest AS (
    -- Keep the latest row per track_id, is_skipped_label
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        ms_played_bucket,
        is_skipped_label,
        device_prefix,
        max_ms_played_bucket,
        ms_played_bucket_minus_max
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id, is_skipped_label ORDER BY ms_played_bucket DESC, id DESC) AS rn
        FROM without_matches
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        source,
        ms_played_bucket,
        is_skipped_label,
        device_prefix,
        max_ms_played_bucket,
        ms_played_bucket_minus_max,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id,
        LEAD(ms_played_bucket_minus_max) OVER (ORDER BY max_ms_played_bucket DESC, id) AS next_ms_played_bucket_minus_max,
        SUM(max_ms_played_bucket) OVER w_listener_id AS listener_id_sum_max_ms_played_bucket,
        LAG(ms_played_bucket, 3) OVER w_all_by_max_ms_played_bucket AS prev3_ms_played_bucket,
        NTILE(4) OVER (PARTITION BY track_id ORDER BY ms_played_bucket_minus_max, id) AS ms_played_bucket_minus_max_quartile
    FROM latest
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_all_by_max_ms_played_bucket AS (ORDER BY max_ms_played_bucket DESC, id)
),
with_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.source,
        s.is_skipped_label,
        s.device_prefix,
        s.max_ms_played_bucket,
        s.ms_played_bucket_minus_max,
        s.next_ms_played_bucket_minus_max,
        s.listener_id_sum_max_ms_played_bucket,
        s.prev3_ms_played_bucket,
        l.interests AS listener_interests,
        l.country AS listener_country,
        l.username AS listener_username
    FROM with_windows s
    JOIN listeners l ON l.id = s.listener_id
)
SELECT id, metric, metric_value
FROM (
    SELECT id, ms_played_bucket_minus_max, next_ms_played_bucket_minus_max
    FROM with_details_listener
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (ms_played_bucket_minus_max, next_ms_played_bucket_minus_max)
) unp
