WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        LOWER(TRIM(device)) AS device_clean,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE source = 'radio'
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        track_id,
        played_at,
        is_skipped,
        played_at_month,
        device_clean,
        played_at_date,
        DATE_FORMAT(played_at_month, 'yyyy-MM-dd') AS played_at_month_label
    FROM base
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        track_id,
        is_skipped,
        device_clean,
        played_at_month_label,
        LENGTH(TRIM(played_at_month_label)) AS played_at_month_label_len
    FROM annotated
    WHERE is_skipped = TRUE
),
with_parent AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.track_id,
        s.is_skipped,
        s.device_clean,
        s.played_at_month_label,
        s.played_at_month_label_len,
        t.release_date AS track_release_date,
        t.moods AS track_moods,
        t.title AS track_title
    FROM tagged s
    LEFT JOIN tracks t ON t.id = s.track_id AND LENGTH(TRIM(t.title)) >= 5
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        is_skipped,
        device_clean,
        played_at_month_label,
        played_at_month_label_len,
        track_release_date,
        track_moods,
        track_title,
        LAST_VALUE(track_release_date) IGNORE NULLS OVER (PARTITION BY played_at_month_label_len, is_skipped ORDER BY track_release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_track_release_date,
        DENSE_RANK() OVER (PARTITION BY played_at_month_label_len, is_skipped ORDER BY track_release_date DESC) AS track_release_date_dense_rank,
        LEAD(played_at_month_label_len, 3) OVER (PARTITION BY played_at_month_label_len, is_skipped ORDER BY track_release_date DESC, id) AS next3_played_at_month_label_len,
        ROW_NUMBER() OVER (PARTITION BY played_at_month_label_len, is_skipped ORDER BY track_release_date DESC, id) AS track_release_date_seq,
        COUNT(*) OVER (PARTITION BY is_skipped, played_at_month_label_len) AS rows_in_is_skipped_played_at_month_label_len,
        NTH_VALUE(track_release_date, 2) OVER (PARTITION BY played_at_month_label_len, is_skipped ORDER BY track_release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_release_date
    FROM with_parent
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        QUARTER(track_release_date) AS track_release_date_quarter,
        ADD_MONTHS(track_release_date, 3) AS track_release_date_plus_3m,
        SUBSTRING(TRIM(device_clean), 1, 3) AS device_clean_prefix
    FROM ranked
    WHERE rows_in_is_skipped_played_at_month_label_len BETWEEN 7 AND 16
)
SELECT
    id,
    is_skipped,
    played_at_month_label,
    played_at_month_label_len,
    track_moods,
    track_title,
    track_release_date_dense_rank,
    next3_played_at_month_label_len,
    track_release_date_seq,
    second_track_release_date,
    track_release_date_quarter,
    track_release_date_plus_3m,
    device_clean_prefix,
    CONCAT_WS('|', played_at_month_label, track_title) AS played_at_month_label_track_title_label,
    DATE_FORMAT(track_release_date, 'yyyy-MM') AS track_release_date_label
FROM scored
ORDER BY track_release_date_dense_rank DESC NULLS FIRST, device_clean_prefix, id
LIMIT 10
