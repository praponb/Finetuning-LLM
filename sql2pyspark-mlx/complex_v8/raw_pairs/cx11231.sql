WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE bpm <= 127
      AND duration_sec BETWEEN 154 AND 352
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        a.country AS artist_country,
        a.artist_name,
        a.genre AS artist_genre
    FROM base s
    JOIN artists a ON a.id = s.artist_id
),
artist_country_stats AS (
    -- Avg duration_sec per artist_country
    SELECT artist_country, AVG(duration_sec) AS avg_duration_sec
    FROM with_details_artist
    GROUP BY artist_country
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_duration_sec,
        ROUND(s.duration_sec - g.avg_duration_sec, 2) AS duration_sec_minus_avg
    FROM with_details_artist s
    JOIN artist_country_stats g ON g.artist_country = s.artist_country
    WHERE s.duration_sec > g.avg_duration_sec
)
SELECT id, field, reading
FROM (
    SELECT id, avg_duration_sec, duration_sec_minus_avg
    FROM vs_group
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (avg_duration_sec, duration_sec_minus_avg)
) unp
