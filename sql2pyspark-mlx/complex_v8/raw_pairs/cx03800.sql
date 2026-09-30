WITH recent_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        bpm - duration_sec AS bpm_minus_duration_sec
    FROM tracks
    WHERE bpm >= 76
      AND duration_sec > 144
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(bpm_minus_duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS bpm_minus_duration_sec_sum_2,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id) AS seq_no,
        ROUND(bpm_minus_duration_sec - AVG(bpm_minus_duration_sec) OVER w_artist_id, 2) AS bpm_minus_duration_sec_vs_artist_id_avg,
        SUM(bpm_minus_duration_sec) OVER (PARTITION BY artist_id) AS artist_id_sum_bpm_minus_duration_sec
    FROM recent_tracks
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
joined_artist AS (
    -- Join artists
    SELECT
        s.*,
        a.genre AS artist_genre,
        a.artist_name,
        a.country AS artist_country
    FROM with_trends s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre IN ('Indie', 'HipHop', 'Pop')
)
SELECT
    COALESCE(is_explicit_label, 'ALL') AS is_explicit_label,
    COALESCE(artist_country, 'ALL') AS artist_country,
    GROUPING(is_explicit_label) AS is_total_is_explicit_label,
    COUNT(*) AS row_count
FROM joined_artist
GROUP BY GROUPING SETS ((is_explicit_label, artist_country), (is_explicit_label), ())
ORDER BY is_explicit_label NULLS LAST, artist_country NULLS LAST
