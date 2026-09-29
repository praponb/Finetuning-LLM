WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        LENGTH(TRIM(title)) AS title_len,
        LOWER(TRIM(title)) AS title_clean,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label_2
    FROM scoped
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, title_len, duration_sec_filled
    FROM enriched
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (title_len AS title_len_v, duration_sec_filled AS m_duration_sec_filled)
) unp
