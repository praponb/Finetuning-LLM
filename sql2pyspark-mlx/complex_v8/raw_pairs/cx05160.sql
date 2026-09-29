WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'G%'
      AND artist_id IS NOT NULL
),
latest AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        moods
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id DESC) AS recency_rank
        FROM prepared
    ) ranked
    WHERE recency_rank = 1
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        is_explicit,
        artist_id,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        COUNT(*) FILTER (WHERE UPPER(TRIM(title)) LIKE 'S%') AS n_upper_trim_title_like_s,
        COUNT(*) FILTER (WHERE title IS NOT NULL) AS n_title_is_not_null,
        COUNT(*) AS row_count
    FROM latest
    GROUP BY is_explicit, artist_id, DATE_TRUNC('MONTH', release_date)
)
-- Native SQL UNPIVOT (wide to long)
SELECT is_explicit, artist_id, field, reading
FROM (
    SELECT is_explicit, artist_id, n_title_is_not_null, distinct_artist_ids, n_upper_trim_title_like_s
    FROM grouped
) src
UNPIVOT (
    reading FOR field IN (n_title_is_not_null AS m_n_title_is_not_null, distinct_artist_ids AS distinct_artist_ids_v, n_upper_trim_title_like_s AS n_upper_trim_title_like_s_v)
) unp
ORDER BY is_explicit, artist_id, field
