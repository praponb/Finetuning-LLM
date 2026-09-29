WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND bpm >= 76
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(bpm) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        ROW_NUMBER() OVER w_all_by_bpm AS rn,
        MAX(bpm) OVER (PARTITION BY is_explicit) AS is_explicit_max_bpm,
        MAX(bpm) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS bpm_max_2,
        MAX(bpm) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_so_far,
        ROUND(AVG(bpm) OVER (ORDER BY bpm DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_3_prior
    FROM prepared
    WINDOW w_all_by_bpm AS (ORDER BY bpm DESC, id)
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.running_bpm,
        s.rn,
        s.is_explicit_max_bpm,
        s.bpm_max_2,
        s.max_bpm_so_far,
        s.bpm_avg_3_prior,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM with_trends s
    JOIN artists a ON a.id = s.artist_id
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.running_bpm,
        s.rn,
        s.is_explicit_max_bpm,
        s.bpm_max_2,
        s.max_bpm_so_far,
        s.bpm_avg_3_prior,
        s.artist_is_verified,
        s.artist_country,
        s.artist_debut_year,
        a.is_verified AS artist_is_verified_2,
        a.country AS artist_country_2
    FROM enriched_artist s
    LEFT JOIN artists a ON a.id = s.artist_id
)
-- Subtotals with GROUPING SETS
SELECT
    artist_is_verified_2,
    COALESCE(artist_country, 'ALL') AS artist_country,
    ROUND(SUM(running_bpm) / NULLIF(SUM(artist_debut_year), 0), 3) AS running_bpm_to_artist_debut_year_ratio
FROM with_parent_artist
GROUP BY GROUPING SETS ((artist_is_verified_2, artist_country), (artist_is_verified_2))
ORDER BY artist_is_verified_2 NULLS LAST, artist_country NULLS LAST
