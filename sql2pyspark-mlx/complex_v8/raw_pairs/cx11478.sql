WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE bpm < 142
),
joined AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.moods,
        s.title_slug,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year BETWEEN 1996 AND 2006
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(artist_debut_year) OVER (PARTITION BY artist_is_verified) AS artist_is_verified_max_artist_debut_year,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_is_verified), 2) AS artist_is_verified_avg_artist_debut_year,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER (PARTITION BY artist_is_verified), 2) AS artist_debut_year_vs_artist_is_verified_avg,
        FIRST_VALUE(artist_debut_year) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_artist_debut_year,
        MAX(artist_debut_year) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_artist_debut_year_so_far,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_is_verified), 2) AS duration_sec_vs_artist_is_verified_avg,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER (PARTITION BY artist_is_verified), 0), 4) AS artist_debut_year_share_of_artist_is_verified
    FROM joined
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        title_slug,
        artist_debut_year,
        artist_is_verified,
        artist_is_verified_max_artist_debut_year,
        artist_is_verified_avg_artist_debut_year,
        artist_debut_year_vs_artist_is_verified_avg,
        first_artist_debut_year,
        SUBSTRING(TRIM(title_slug), 1, 3) AS title_slug_prefix,
        LEAST(duration_sec, first_artist_debut_year) AS least_duration_sec_first_artist_debut_year,
        SUBSTRING(TRIM(title_slug), 1, 2) AS title_slug_prefix_2
    FROM ranked
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        artist_is_verified,
        SUM(first_artist_debut_year) AS total_first_artist_debut_year,
        PERCENTILE_APPROX(artist_debut_year, 0.5) AS median_artist_debut_year
    FROM annotated
    GROUP BY artist_id, artist_is_verified
)
SELECT
    artist_id,
    median_artist_debut_year,
    CASE WHEN median_artist_debut_year >= 2010 THEN 'large' WHEN median_artist_debut_year >= 2000 THEN 'medium' ELSE 'small' END AS median_artist_debut_year_tier,
    ABS(median_artist_debut_year - 2005) AS median_artist_debut_year_distance
FROM rollup_base
ORDER BY median_artist_debut_year_tier DESC, median_artist_debut_year DESC
