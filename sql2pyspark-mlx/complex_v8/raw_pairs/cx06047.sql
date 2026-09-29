WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
    FROM tracks
    WHERE bpm <= 142
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM prepared_tracks
    WHERE is_explicit_flag < (SELECT MAX(is_explicit_flag) FROM prepared_tracks)
),
scored AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(release_date_label)) AS release_date_label_len
    FROM qualified
),
deduped AS (
    -- Keep the latest row per is_explicit_label, artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        is_explicit_flag,
        is_explicit_label,
        release_date_label,
        release_date_label_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY is_explicit_label, artist_id ORDER BY release_date_label_len DESC, id DESC) AS recency_rank
        FROM scored
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    release_date_label_len,
    is_explicit_label,
    artist_id,
    ROUND(SUM(bpm) / NULLIF(SUM(is_explicit_flag), 0), 3) AS bpm_to_is_explicit_flag_ratio,
    COUNT(*) FILTER (WHERE LOWER(title) LIKE '%ng%') AS n_lower_title_like_ng,
    COUNT(DISTINCT artist_id) AS distinct_artist_ids
FROM deduped
GROUP BY ROLLUP(release_date_label_len, is_explicit_label, artist_id)
