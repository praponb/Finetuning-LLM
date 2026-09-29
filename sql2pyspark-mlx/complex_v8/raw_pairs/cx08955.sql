WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE LOWER(title) LIKE '%in%'
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.id) AS first_played_at
    FROM valid s
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        first_played_at,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_bpm,
        ROUND((UNIX_TIMESTAMP(first_played_at) - UNIX_TIMESTAMP(LAG(first_played_at) OVER (PARTITION BY artist_id ORDER BY first_played_at, id))) / 60, 1) AS minutes_since_prev_first_played_at,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC NULLS FIRST, id) AS rn,
        bpm - LAG(bpm) OVER (PARTITION BY artist_id ORDER BY first_played_at, id) AS bpm_change,
        MAX(bpm) OVER (PARTITION BY artist_id ORDER BY bpm DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_max_6_prior
    FROM summarized_rows
),
combined_details_artist AS (
    -- Attach artist attributes
    SELECT s.*, a.debut_year AS artist_debut_year, a.artist_name
    FROM ranked s
    JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(artist_debut_year) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_artist_debut_year,
        MAX(bpm_change) OVER (PARTITION BY MONTH(first_played_at)) AS first_played_at_month_max_bpm_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', first_played_at), rn ORDER BY artist_id_avg_bpm DESC, id), 3) AS artist_id_avg_bpm_percent_rank
    FROM combined_details_artist
),
derived AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(first_played_at, 'yyyy-MM-dd') AS first_played_at_label
    FROM with_trends
)
SELECT
    artist_id,
    is_explicit,
    COUNT(*) AS cnt
FROM derived
GROUP BY CUBE(artist_id, is_explicit)
ORDER BY artist_id NULLS LAST, is_explicit NULLS LAST
