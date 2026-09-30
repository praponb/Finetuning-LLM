WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        INITCAP(TRIM(title)) AS title_clean,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE LOWER(title) LIKE '%ne%'
      AND bpm BETWEEN 73 AND 147
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.moods,
        s.release_date_label,
        s.duration_sec_filled,
        a.country AS artist_country
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(duration_sec_filled / NULLIF(SUM(duration_sec_filled) OVER (PARTITION BY artist_id), 0), 4) AS duration_sec_filled_share_of_artist_id,
        DENSE_RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec_filled ASC) AS duration_sec_filled_dense_rank,
        duration_sec_filled - LAG(duration_sec_filled) OVER w_artist_id_is_explicit_by_dur AS duration_sec_filled_change,
        ROUND(AVG(duration_sec_filled) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec_filled, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS duration_sec_filled_avg_3
    FROM enriched_artist
    WINDOW w_artist_id_is_explicit_by_dur AS (PARTITION BY artist_id, is_explicit ORDER BY duration_sec_filled, id)
)
-- Final projection
SELECT
    id,
    title,
    duration_sec_filled,
    artist_country,
    duration_sec_filled_share_of_artist_id,
    duration_sec_filled_dense_rank,
    duration_sec_filled_change,
    duration_sec_filled_avg_3
FROM metrics
ORDER BY duration_sec_filled_change DESC, duration_sec_filled_share_of_artist_id DESC NULLS FIRST
