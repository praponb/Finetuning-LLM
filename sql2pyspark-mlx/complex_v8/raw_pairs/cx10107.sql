WITH scoped AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        ABS(bpm - 142) AS bpm_distance
    FROM tracks
),
combined_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.bpm_distance,
        a.country AS artist_country,
        a.artist_name,
        a.debut_year AS artist_debut_year
    FROM scoped s
    JOIN artists a ON a.id = s.artist_id
    WHERE LENGTH(TRIM(a.artist_name)) >= 7
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        is_explicit,
        bpm,
        bpm_distance,
        artist_country,
        artist_name,
        LEAD(bpm, 2) OVER (PARTITION BY artist_country ORDER BY artist_debut_year, id) AS next2_bpm,
        COUNT(*) OVER w_artist_id_artist_country AS rows_in_artist_id_artist_country,
        duration_sec - LAG(duration_sec) OVER w_artist_id_by_bpm_distance AS duration_sec_change,
        NTH_VALUE(artist_debut_year, 2) OVER (PARTITION BY artist_country ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year
    FROM combined_details_artist
    WINDOW w_artist_id_artist_country AS (PARTITION BY artist_id, artist_country), w_artist_id_by_bpm_distance AS (PARTITION BY artist_id ORDER BY bpm_distance ASC, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(artist_name), 1, 2) AS artist_name_prefix,
        CONCAT_WS(': ', artist_name, artist_country) AS artist_name_artist_country_label
    FROM ranked
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        bpm,
        artist_country,
        artist_name,
        duration_sec_change,
        artist_name_prefix,
        FLOOR(bpm_distance / 25) * 25 AS bpm_distance_bucket,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tagged
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.device IS NOT NULL
              AND p.played_at < TIMESTAMP '2025-03-02 09:00:00'
        )
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_activity
    WHERE duration_sec_change > (SELECT AVG(duration_sec_change) FROM with_activity)
)
SELECT
    id,
    bpm,
    artist_country,
    artist_name,
    duration_sec_change,
    artist_name_prefix,
    bpm_distance_bucket,
    is_explicit_label
FROM qualified
ORDER BY artist_name
