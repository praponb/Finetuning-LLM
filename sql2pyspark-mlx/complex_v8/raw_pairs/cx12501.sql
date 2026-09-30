WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        CASE WHEN device IN ('tv', 'speaker') THEN 'group_a' WHEN device = 'desktop' THEN 'group_b' ELSE 'other' END AS device_group,
        CASE WHEN device IN ('car', 'speaker') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group_2,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
    FROM plays
    WHERE source NOT IN ('playlist', 'radio')
      AND listener_id <= 13
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN device_group IN ('other', 'group_b') THEN 'group_a' WHEN device_group = 'group_a' THEN 'group_b' ELSE 'other' END AS device_group_group,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        LOWER(REGEXP_REPLACE(TRIM(played_at_label), ' ', '_')) AS played_at_label_slug
    FROM base
    WHERE track_id >= 16
),
latest AS (
    -- Keep the earliest row per source, device_group_group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        device_group,
        device_group_2,
        played_at_label,
        device_group_group,
        is_skipped_label,
        played_at_label_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source, device_group_group ORDER BY played_at, id) AS recency_rank
        FROM scored
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    is_skipped,
    source,
    device_group,
    device_group_2,
    device_group_group,
    LENGTH(TRIM(played_at_label)) AS played_at_label_len,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label_2
FROM latest
WHERE device_group_2 IN ('group_b', 'group_a')
ORDER BY id DESC NULLS FIRST, device_group_2 DESC
LIMIT 10
