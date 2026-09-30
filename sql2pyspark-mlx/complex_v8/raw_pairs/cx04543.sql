WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE duration_sec >= 165
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('WEEK', release_date) AS release_date_week,
        LENGTH(TRIM(title)) AS title_len
    FROM prepared
    WHERE is_explicit_label = 'no'
)
SELECT id, metric, metric_value
FROM (
    SELECT id, duration_sec, title_len
    FROM annotated
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (duration_sec AS m_duration_sec, title_len AS title_len_v)
) unp
