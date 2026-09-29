WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        LOWER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE artist_id >= 9
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC NULLS FIRST, id) AS release_date_rank,
        LAG(release_date, 3) OVER w_release_date_month_artist_id AS prev3_release_date,
        DENSE_RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC NULLS FIRST) AS release_date_dense_rank,
        LAST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        FIRST_VALUE(release_date) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date
    FROM recent
    WINDOW w_release_date_month_artist_id AS (PARTITION BY MONTH(release_date), artist_id ORDER BY release_date, id)
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.genre AS artist_genre,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM with_windows s
    JOIN artists a ON a.id = s.artist_id
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS last_track_release_date,
        (SELECT ROUND(AVG(t.bpm), 2) FROM tracks t WHERE t.artist_id = s.artist_id) AS avg_track_bpm,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count
    FROM with_parent s
)
-- Subtotals with GROUPING SETS
SELECT
    track_count,
    COALESCE(artist_country, 'ALL') AS artist_country,
    GROUPING(track_count) AS is_total_track_count,
    MAX(track_count) AS max_track_count
FROM summarized_rows
GROUP BY GROUPING SETS ((track_count, artist_country), (track_count))
ORDER BY track_count NULLS LAST, artist_country NULLS LAST
