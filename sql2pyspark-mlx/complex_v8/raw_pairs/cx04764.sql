WITH prepared_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        genre,
        country,
        debut_year,
        is_verified,
        ABS(debut_year - 2006) AS debut_year_distance,
        CASE WHEN genre IN ('Classical', 'Indie') THEN 'group_a' WHEN genre = 'Pop' THEN 'group_b' ELSE 'other' END AS genre_group,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown
    FROM artists
    WHERE debut_year BETWEEN 1994 AND 2023
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MAX(release_date) AS last_track_release_date,
        MAX(release_date) AS last_track_release_date_2,
        MIN(bpm) AS min_track_bpm
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.last_track_release_date,
        t.last_track_release_date_2,
        t.min_track_bpm
    FROM prepared_artists s
    LEFT JOIN track_stats t ON t.artist_id = s.id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(min_track_bpm) OVER (PARTITION BY country_or_unknown, is_verified) AS country_or_unknown_is_verified_sum_min_track_bpm,
        ROUND(CUME_DIST() OVER (PARTITION BY genre, country_or_unknown ORDER BY last_track_release_date_2), 3) AS last_track_release_date_2_cume_dist,
        NTILE(4) OVER (PARTITION BY genre, country_or_unknown ORDER BY last_track_release_date_2, id) AS last_track_release_date_2_quartile,
        LAG(last_track_release_date_2, 3) OVER (PARTITION BY genre, country_or_unknown ORDER BY last_track_release_date_2, id) AS prev3_last_track_release_date_2,
        ROUND(min_track_bpm - AVG(min_track_bpm) OVER (PARTITION BY country_or_unknown, is_verified), 2) AS min_track_bpm_vs_country_or_unknown_is_verified_,
        min_track_bpm - LAG(min_track_bpm) OVER (PARTITION BY genre, country_or_unknown ORDER BY last_track_release_date_2, id) AS min_track_bpm_change
    FROM with_counts
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', last_track_release_date) AS last_track_release_date_month,
        CASE WHEN min_track_bpm >= 156 THEN 'HIGH' WHEN min_track_bpm >= 102 THEN 'MEDIUM' ELSE 'LOW' END AS min_track_bpm_tier,
        COUNT(DISTINCT country) AS distinct_countrys,
        ROUND(100 * COUNT(CASE WHEN genre NOT IN ('HipHop', 'Indie') THEN 1 END) / COUNT(*), 1) AS pct_genre_not_in_hiphop_indie,
        COUNT(DISTINCT country_or_unknown) AS distinct_country_or_unknowns,
        ARRAY_SORT(COLLECT_SET(genre)) AS genre_list,
        COUNT(*) AS n_rows
    FROM ranked
    GROUP BY DATE_TRUNC('MONTH', last_track_release_date), CASE WHEN min_track_bpm >= 156 THEN 'HIGH' WHEN min_track_bpm >= 102 THEN 'MEDIUM' ELSE 'LOW' END
),
exploded AS (
    -- One row per genre_list_item
    SELECT
        s.last_track_release_date_month,
        s.min_track_bpm_tier,
        s.distinct_countrys,
        s.pct_genre_not_in_hiphop_indie,
        s.distinct_country_or_unknowns,
        s.n_rows,
        t.genre_list_item
    FROM summary s
    LATERAL VIEW EXPLODE(s.genre_list) t AS genre_list_item
)
-- Final projection
SELECT
    last_track_release_date_month,
    min_track_bpm_tier,
    pct_genre_not_in_hiphop_indie,
    distinct_country_or_unknowns,
    genre_list_item
FROM exploded
ORDER BY genre_list_item DESC, last_track_release_date_month, min_track_bpm_tier
LIMIT 5
