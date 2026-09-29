WITH base_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        is_verified,
        LENGTH(TRIM(artist_name)) AS artist_name_len,
        CASE WHEN country IN ('JP', 'TH') THEN 'group_a' WHEN country = 'KR' THEN 'group_b' ELSE 'other' END AS country_group
    FROM artists
    WHERE is_verified = FALSE
      AND genre NOT IN ('Classical', 'Jazz')
),
screened AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM base_artists s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.is_explicit = TRUE
              AND t.duration_sec >= 165
        )
),
country_group_stats AS (
    -- Median debut_year per country_group
    SELECT country_group, PERCENTILE_APPROX(debut_year, 0.5) AS median_debut_year
    FROM screened
    GROUP BY country_group
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_debut_year,
        ROUND(s.debut_year - g.median_debut_year, 2) AS debut_year_minus_median
    FROM screened s
    JOIN country_group_stats g ON g.country_group = s.country_group
    WHERE s.debut_year >= g.median_debut_year
),
with_activity AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM above_average s
    WHERE s.id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE t.release_date BETWEEN DATE '2024-08-26' AND DATE '2025-11-14'
              AND SIZE(t.moods) >= 2
        )
      AND s.country IN ('JP', 'KR', 'TH')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY artist_name_len, country_group ORDER BY debut_year DESC, id) AS debut_year_seq,
        NTH_VALUE(artist_name_len, 2) OVER (PARTITION BY country, is_verified ORDER BY median_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_name_len,
        RANK() OVER (PARTITION BY artist_name_len, country_group ORDER BY debut_year DESC) AS debut_year_rank,
        ROUND(PERCENT_RANK() OVER w_country_is_verified_by_media_2, 3) AS median_debut_year_percent_rank
    FROM with_activity
    WINDOW w_country_is_verified_by_media_2 AS (PARTITION BY country, is_verified ORDER BY median_debut_year DESC, id)
),
per_group AS (
    -- Summarise per group
    SELECT
        debut_year_seq,
        ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list,
        ROUND(AVG(median_debut_year), 2) AS avg_median_debut_year
    FROM with_windows
    GROUP BY debut_year_seq
    HAVING AVG(artist_name_len) > 5
)
SELECT
    debut_year_seq,
    CASE WHEN debut_year_seq >= 6 THEN 'large' WHEN debut_year_seq >= 3 THEN 'medium' ELSE 'small' END AS debut_year_seq_tier
FROM per_group
ORDER BY debut_year_seq_tier, debut_year_seq
