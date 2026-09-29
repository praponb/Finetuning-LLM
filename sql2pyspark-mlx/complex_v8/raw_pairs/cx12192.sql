WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        DATE_TRUNC('YEAR', release_date) AS release_date_year
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'P%'
      AND artist_id <= 13
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, YEAR(release_date) ORDER BY release_date_year DESC), 3) AS release_date_year_percent_rank,
        DENSE_RANK() OVER w_artist_id_release_date_year AS release_date_year_dense_rank
    FROM cleaned_tracks
    WINDOW w_artist_id_release_date_year AS (PARTITION BY artist_id, YEAR(release_date) ORDER BY release_date_year DESC)
),
joined AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.release_date_year_percent_rank,
        s.release_date_year_dense_rank,
        a.is_verified AS artist_is_verified,
        a.artist_name
    FROM sequenced s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.artist_name IS NOT NULL
),
artist_id_stats AS (
    -- Avg duration_sec per artist_id
    SELECT artist_id, AVG(duration_sec) AS avg_duration_sec
    FROM joined
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_duration_sec,
        ROUND(s.duration_sec - g.avg_duration_sec, 2) AS duration_sec_minus_avg
    FROM joined s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.duration_sec > g.avg_duration_sec
)
SELECT
    artist_id,
    release_date_year_dense_rank,
    artist_is_verified,
    SUM(duration_sec) AS total_duration_sec,
    MIN(release_date) AS first_release_date
FROM above_average
GROUP BY GROUPING SETS ((artist_id, release_date_year_dense_rank, artist_is_verified), (artist_id), (release_date_year_dense_rank))
ORDER BY artist_id NULLS LAST, release_date_year_dense_rank NULLS LAST, artist_is_verified NULLS LAST
