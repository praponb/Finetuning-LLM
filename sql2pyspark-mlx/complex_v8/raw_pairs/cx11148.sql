WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-09-26' AND DATE '2025-08-02'
      AND is_explicit = TRUE
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.title_ref,
        s.release_date_label,
        s.least_bpm_duration_sec,
        a.is_verified AS artist_is_verified
    FROM cleaned s
    LEFT JOIN artists a ON a.id = s.artist_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        is_explicit,
        artist_id,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        SUM(duration_sec) AS total_duration_sec,
        ROUND(SUM(duration_sec) / NULLIF(SUM(least_bpm_duration_sec), 0), 3) AS duration_sec_to_least_bpm_duration_sec_ratio,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids
    FROM with_parent_artist
    GROUP BY 1, 2, 3
),
labeled AS (
    -- Label and score rows
    SELECT
        is_explicit,
        artist_id,
        release_date_month,
        total_duration_sec,
        distinct_artist_ids,
        YEAR(release_date_month) AS release_date_month_year
    FROM rollup_base
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.is_explicit,
        s.artist_id,
        s.release_date_month,
        s.total_duration_sec,
        s.distinct_artist_ids,
        a.country AS artist_country
    FROM labeled s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        is_explicit,
        artist_id,
        release_date_month,
        artist_country,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit, distinct_artist_ids)) AS distinct_artist_id_in_is_explicit_distinct_artis,
        MIN(distinct_artist_ids) OVER (PARTITION BY artist_country ORDER BY total_duration_sec, is_explicit, artist_id, release_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_distinct_artist_ids_so_far,
        RANK() OVER (PARTITION BY artist_country ORDER BY total_duration_sec) AS total_duration_sec_rank,
        MIN(distinct_artist_ids) OVER w_is_explicit_distinct_artist AS is_explicit_distinct_artist_ids_min_distinct_art
    FROM joined
    WINDOW w_is_explicit_distinct_artist AS (PARTITION BY is_explicit, distinct_artist_ids)
),
with_details_artist AS (
    -- Join artists
    SELECT
        s.is_explicit,
        s.artist_id,
        s.release_date_month,
        s.distinct_artist_id_in_is_explicit_distinct_artis,
        s.min_distinct_artist_ids_so_far,
        s.total_duration_sec_rank,
        s.is_explicit_distinct_artist_ids_min_distinct_art,
        a.country AS artist_country_2,
        a.is_verified AS artist_is_verified
    FROM windowed s
    JOIN artists a ON a.id = s.artist_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_artist_id_in_is_explicit_distinct_artis, artist_country_2, total_duration_sec_rank
    FROM with_details_artist
) src
PIVOT (
    MAX(total_duration_sec_rank)
    FOR artist_country_2 IN ('US', 'JP', 'BR')
)
ORDER BY distinct_artist_id_in_is_explicit_distinct_artis
