WITH prepared AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CONCAT_WS(' / ', genre, country) AS genre_country_label,
        IF(is_verified, 'yes', 'no') AS is_verified_label,
        CASE WHEN debut_year >= 2018 THEN 'A' WHEN debut_year >= 1997 THEN 'B' ELSE 'C' END AS debut_year_tier
    FROM artists
    WHERE LOWER(artist_name) LIKE '%li%'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        debut_year,
        is_verified,
        is_verified_label,
        debut_year_tier,
        ROUND(debut_year - AVG(debut_year) OVER (PARTITION BY is_verified, debut_year_tier), 2) AS debut_year_vs_is_verified_debut_year_tier_avg,
        SUM(debut_year) OVER (PARTITION BY is_verified_label ORDER BY debut_year DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS debut_year_sum_3_prior,
        LAST_VALUE(debut_year) OVER (PARTITION BY is_verified_label ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_debut_year,
        SIZE(COLLECT_SET(genre) OVER w_is_verified_debut_year_tier) AS distinct_genre_in_is_verified_debut_year_tier
    FROM prepared
    WINDOW w_is_verified_debut_year_tier AS (PARTITION BY is_verified, debut_year_tier)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        country,
        debut_year,
        debut_year_tier,
        debut_year_vs_is_verified_debut_year_tier_avg,
        debut_year_sum_3_prior,
        distinct_genre_in_is_verified_debut_year_tier,
        SUM(last_debut_year) OVER (PARTITION BY is_verified_label, is_verified ORDER BY debut_year_vs_is_verified_debut_year_tier_avg DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last_debut_year_sum_7,
        last_debut_year - LAG(last_debut_year) OVER w_is_verified_label_is_verifie_2 AS last_debut_year_change,
        ROUND(debut_year / NULLIF(SUM(debut_year) OVER (PARTITION BY is_verified), 0), 4) AS debut_year_share_of_is_verified,
        COUNT(*) OVER (PARTITION BY is_verified_label, is_verified ORDER BY debut_year_vs_is_verified_debut_year_tier_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(10) OVER (PARTITION BY is_verified_label, is_verified ORDER BY debut_year_vs_is_verified_debut_year_tier_avg DESC, id) AS debut_year_vs_is_verified_debut_year_tier_avg_de
    FROM with_windows
    WINDOW w_is_verified_label_is_verifie_2 AS (PARTITION BY is_verified_label, is_verified ORDER BY debut_year_vs_is_verified_debut_year_tier_avg DESC, id)
),
debut_year_tier_stats AS (
    -- Max distinct_genre_in_is_verified_debut_year_tier per debut_year_tier
    SELECT debut_year_tier, MAX(distinct_genre_in_is_verified_debut_year_tier) AS max_distinct_genre_in_is_verified_debut_year_tie
    FROM sequenced
    GROUP BY debut_year_tier
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_distinct_genre_in_is_verified_debut_year_tie,
        ROUND(s.distinct_genre_in_is_verified_debut_year_tier - g.max_distinct_genre_in_is_verified_debut_year_tie, 2) AS distinct_genre_in_is_verified_debut_year_tier_mi
    FROM sequenced s
    JOIN debut_year_tier_stats g ON g.debut_year_tier = s.debut_year_tier
    WHERE s.distinct_genre_in_is_verified_debut_year_tier = g.max_distinct_genre_in_is_verified_debut_year_tie
),
detail_rows_tracks AS (
    -- One row per track
    SELECT
        s.id,
        s.country,
        s.debut_year,
        s.debut_year_tier,
        s.debut_year_vs_is_verified_debut_year_tier_avg,
        s.debut_year_sum_3_prior,
        s.distinct_genre_in_is_verified_debut_year_tier,
        s.last_debut_year_sum_7,
        s.last_debut_year_change,
        s.running_count,
        s.debut_year_vs_is_verified_debut_year_tier_avg_de,
        s.max_distinct_genre_in_is_verified_debut_year_tie,
        s.distinct_genre_in_is_verified_debut_year_tier_mi,
        t.id AS track_id,
        t.is_explicit AS track_is_explicit,
        t.release_date AS track_release_date,
        t.bpm AS track_bpm,
        t.title AS track_title
    FROM outliers s
    JOIN tracks t ON t.artist_id = s.id AND t.bpm >= 76
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(*) AS track_count,
        MAX(duration_sec) AS max_track_duration_sec,
        MAX(release_date) AS last_track_release_date
    FROM tracks
    GROUP BY artist_id
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count,
        t.max_track_duration_sec,
        t.last_track_release_date
    FROM detail_rows_tracks s
    LEFT JOIN track_stats t ON t.artist_id = s.id
)
-- Final projection
SELECT
    id,
    country,
    debut_year,
    debut_year_tier,
    debut_year_vs_is_verified_debut_year_tier_avg,
    debut_year_sum_3_prior,
    last_debut_year_sum_7,
    last_debut_year_change,
    running_count,
    debut_year_vs_is_verified_debut_year_tier_avg_de,
    max_distinct_genre_in_is_verified_debut_year_tie,
    distinct_genre_in_is_verified_debut_year_tier_mi,
    track_id,
    track_release_date,
    track_bpm,
    track_title,
    track_count,
    last_track_release_date
FROM summarized_rows
ORDER BY debut_year_tier, id, track_id
LIMIT 10
