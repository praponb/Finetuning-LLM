WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref
    FROM tracks
    WHERE duration_sec < 409
),
with_details AS (
    -- Join artists
    SELECT s.*, a.debut_year AS artist_debut_year
    FROM filtered_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_artist_debut_year,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY duration_sec, id) AS duration_sec_change,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        FIRST_VALUE(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_artist_debut_year,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        ROW_NUMBER() OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY duration_sec, id) AS seq_no,
        LEAD(release_date, 3) OVER w_release_date_year_artist_id AS next3_release_date,
        NTILE(3) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id) AS duration_sec_tercile,
        RANK() OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY duration_sec) AS duration_sec_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY duration_sec), 3) AS duration_sec_percent_rank
    FROM with_details
    WINDOW w_release_date_year_artist_id AS (PARTITION BY YEAR(release_date), artist_id ORDER BY duration_sec, id)
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    bpm,
    title_ref,
    artist_debut_year,
    artist_id_avg_artist_debut_year,
    bpm_share_of_artist_id,
    duration_sec_change,
    rows_in_artist_id,
    seq_no,
    duration_sec_tercile,
    duration_sec_rank
FROM ranked
ORDER BY seq_no DESC
