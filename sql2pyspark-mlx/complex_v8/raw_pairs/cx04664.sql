WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE bpm > 100
),
joined AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.country AS artist_country,
        a.artist_name,
        a.is_verified AS artist_is_verified
    FROM prepared_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        release_date,
        is_explicit,
        bpm,
        moods,
        artist_country,
        artist_name,
        artist_is_verified,
        NTILE(10) OVER w_all_by_release_date AS release_date_decile,
        ROUND(PERCENT_RANK() OVER w_all_by_release_date, 3) AS release_date_percent_rank,
        SIZE(COLLECT_SET(artist_country) OVER (PARTITION BY YEAR(release_date))) AS distinct_artist_country_in_release_date_year,
        ROUND(AVG(bpm) OVER w_release_date_year, 2) AS release_date_year_avg_bpm,
        MAX(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_max_3,
        ROUND(AVG(duration_sec) OVER (PARTITION BY YEAR(release_date)), 2) AS release_date_year_avg_duration_sec,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY YEAR(release_date)), 0), 4) AS duration_sec_share_of_release_date_year,
        SUM(bpm) OVER (PARTITION BY YEAR(release_date)) AS release_date_year_sum_bpm,
        SUM(bpm) OVER (ORDER BY release_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_sum_6_prior,
        ROUND(AVG(duration_sec) OVER (ORDER BY release_date DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_4
    FROM joined
    WINDOW w_all_by_release_date AS (ORDER BY release_date DESC, id), w_release_date_year AS (PARTITION BY YEAR(release_date))
),
enriched AS (
    -- Add calculated columns
    SELECT *, SIZE(moods) AS moods_count
    FROM windowed
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(release_date_percent_rank - AVG(release_date_percent_rank) OVER (PARTITION BY moods_count), 2) AS release_date_percent_rank_vs_moods_count_avg,
        FIRST_VALUE(release_date_percent_rank) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY distinct_artist_country_in_release_date_year NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date_percent_rank,
        SUM(release_date_percent_rank) OVER w_distinct_artist_country_in_r AS distinct_artist_country_in_release_date_year_sum
    FROM enriched
    WINDOW w_distinct_artist_country_in_r AS (PARTITION BY distinct_artist_country_in_release_date_year)
),
totals AS (
    -- Group-level totals
    SELECT
        distinct_artist_country_in_release_date_year,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        moods_count,
        SUM(release_date_percent_rank) AS total_release_date_percent_rank,
        ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list
    FROM ranked
    WHERE is_explicit = FALSE
    GROUP BY 1, 2, 3
    HAVING AVG(release_date_year_avg_bpm) > 102.0
)
-- Final projection
SELECT
    distinct_artist_country_in_release_date_year,
    release_date_ym,
    total_release_date_percent_rank,
    artist_name_list,
    CASE WHEN moods_count >= 2 THEN 'A' WHEN moods_count >= 1 THEN 'B' ELSE 'C' END AS moods_count_tier
FROM totals
WHERE moods_count < 2
