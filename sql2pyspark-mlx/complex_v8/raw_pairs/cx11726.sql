WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        QUARTER(release_date) AS release_date_quarter,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND NOT is_explicit
),
without_matches AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.is_explicit = FALSE
              AND t.release_date < DATE '2025-03-08'
        )
),
unique_rows AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        release_date_quarter,
        title_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY title_len DESC, id DESC) AS rn
        FROM without_matches
    ) ranked
    WHERE rn = 1
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(TRIM(title)) AS title_clean,
        MONTH(release_date) AS release_date_month,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM unique_rows
)
SELECT
    release_date_month,
    release_date_quarter,
    ROUND(AVG(bpm), 2) AS avg_bpm,
    COUNT(*) AS cnt
FROM annotated
GROUP BY GROUPING SETS ((release_date_month, release_date_quarter), (release_date_month), ())
ORDER BY release_date_month NULLS LAST, release_date_quarter NULLS LAST
