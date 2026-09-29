WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        SIZE(moods) AS moods_count,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm
    FROM tracks
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.duration_sec_per_bpm,
        a.debut_year AS artist_debut_year,
        a.artist_name,
        a.country AS artist_country
    FROM recent s
    JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(artist_id) OVER w_artist_country_is_explicit) AS distinct_artist_id_in_artist_country_is_explicit,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY is_explicit), 2) AS bpm_vs_is_explicit_avg,
        ROUND(duration_sec_per_bpm - AVG(duration_sec_per_bpm) OVER (PARTITION BY artist_country, is_explicit), 2) AS duration_sec_per_bpm_vs_artist_country_is_explic,
        ROW_NUMBER() OVER w_artist_country_is_explicit_b AS rn,
        duration_sec_per_bpm - LAG(duration_sec_per_bpm) OVER (PARTITION BY is_explicit ORDER BY duration_sec_per_bpm, id) AS duration_sec_per_bpm_change,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit ORDER BY duration_sec_per_bpm, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_1_prior,
        LEAD(duration_sec_per_bpm, 1, 0) OVER (ORDER BY artist_debut_year, id) AS next_duration_sec_per_bpm,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec_per_bpm, id) AS rn_2,
        LEAD(bpm) OVER (ORDER BY artist_debut_year, id) AS next_bpm,
        ROUND(AVG(duration_sec_per_bpm) OVER w_artist_country_is_explicit, 2) AS artist_country_is_explicit_avg_duration_sec_per_,
        SUM(artist_debut_year) OVER (PARTITION BY is_explicit) AS is_explicit_sum_artist_debut_year,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY is_explicit), 2) AS is_explicit_avg_artist_debut_year,
        LAST_VALUE(artist_debut_year) OVER (ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_artist_debut_year,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_country, is_explicit), 2) AS duration_sec_vs_artist_country_is_explicit_avg,
        ROUND(CUME_DIST() OVER (ORDER BY artist_debut_year), 3) AS artist_debut_year_cume_dist
    FROM combined_details
    WINDOW w_artist_country_is_explicit AS (PARTITION BY artist_country, is_explicit), w_artist_country_is_explicit_b AS (PARTITION BY artist_country, is_explicit ORDER BY duration_sec DESC, id)
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    is_explicit,
    bpm,
    moods,
    artist_debut_year,
    artist_name,
    distinct_artist_id_in_artist_country_is_explicit,
    bpm_vs_is_explicit_avg,
    duration_sec_per_bpm_vs_artist_country_is_explic,
    duration_sec_per_bpm_change,
    bpm_avg_1_prior,
    rn_2,
    next_bpm,
    artist_country_is_explicit_avg_duration_sec_per_,
    is_explicit_sum_artist_debut_year,
    is_explicit_avg_artist_debut_year,
    last_artist_debut_year,
    duration_sec_vs_artist_country_is_explicit_avg,
    artist_debut_year_cume_dist,
    last_artist_debut_year + bpm_avg_1_prior AS last_artist_debut_year_plus_bpm_avg_1_prior,
    ABS(artist_debut_year - 2015) AS artist_debut_year_distance
FROM with_windows
ORDER BY artist_id, title DESC
