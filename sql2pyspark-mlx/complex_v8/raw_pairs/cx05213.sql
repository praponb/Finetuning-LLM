WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        YEAR(played_at) AS played_at_year,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE source <> 'playlist'
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source,
        s.played_at_year,
        s.device_slug,
        l.country AS listener_country,
        l.plan AS listener_plan,
        l.interests AS listener_interests,
        t.duration_sec AS track_duration_sec,
        t.is_explicit AS track_is_explicit
    FROM valid_plays s
    JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
    WHERE l.plan = 'family'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(track_duration_sec - AVG(track_duration_sec) OVER (PARTITION BY played_at_year, device), 2) AS track_duration_sec_vs_played_at_year_device_avg,
        ROUND(played_at_year / NULLIF(SUM(played_at_year) OVER w_played_at_year_device, 0), 4) AS played_at_year_share_of_played_at_year_device,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY listener_country, listener_plan)) AS distinct_device_in_listener_country_listener_pla,
        ROUND(track_duration_sec / NULLIF(SUM(track_duration_sec) OVER (PARTITION BY listener_country, listener_plan), 0), 4) AS track_duration_sec_share_of_listener_country_lis,
        DENSE_RANK() OVER w_track_is_explicit_by_track_d AS track_duration_sec_dense_rank,
        SIZE(COLLECT_SET(listener_id) OVER w_played_at_year_device) AS distinct_listener_id_in_played_at_year_device,
        LEAD(played_at, 3) OVER w_all_by_played_at AS next3_played_at,
        MAX(played_at_year) OVER (PARTITION BY listener_country, listener_plan) AS listener_country_listener_plan_max_played_at_yea,
        COUNT(*) OVER w_played_at_year_device AS rows_in_played_at_year_device,
        LAST_VALUE(played_at_year) OVER (PARTITION BY track_is_explicit ORDER BY track_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at_year,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY track_is_explicit ORDER BY track_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        LAG(played_at) OVER w_listener_country_source_by_p AS prev_played_at
    FROM with_parent
    WINDOW w_played_at_year_device AS (PARTITION BY played_at_year, device), w_track_is_explicit_by_track_d AS (PARTITION BY track_is_explicit ORDER BY track_duration_sec DESC, id), w_all_by_played_at AS (ORDER BY played_at ASC, id), w_listener_country_source_by_p AS (PARTITION BY listener_country, source ORDER BY played_at, id)
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM windowed
    WHERE track_duration_sec_dense_rank <= 3
),
stats AS (
    -- Summarise per group
    SELECT
        track_is_explicit,
        listener_country,
        PERCENTILE_APPROX(played_at_year, 0.5) AS median_played_at_year,
        COUNT(DISTINCT listener_country) AS distinct_listener_countrys,
        COUNT(*) FILTER (WHERE source = 'search') AS n_source_search,
        SUM(track_duration_sec_vs_played_at_year_device_avg) FILTER (WHERE listener_plan NOT IN ('student', 'free')) AS track_duration_sec_vs_played_at_year_device_avg_,
        MIN(last_played_at_year) AS min_last_played_at_year,
        MIN(second_played_at) AS first_second_played_at
    FROM top_rows
    GROUP BY track_is_explicit, listener_country
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        track_is_explicit,
        listener_country,
        median_played_at_year,
        n_source_search,
        track_duration_sec_vs_played_at_year_device_avg_,
        min_last_played_at_year,
        SUM(n_source_search) OVER (ORDER BY min_last_played_at_year ASC, track_is_explicit, listener_country ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_n_source_search,
        ROUND(distinct_listener_countrys - AVG(distinct_listener_countrys) OVER (PARTITION BY n_source_search, min_last_played_at_year), 2) AS distinct_listener_countrys_vs_n_source_search_mi,
        SUM(distinct_listener_countrys) OVER w_n_source_search_min_last_pla AS n_source_search_min_last_played_at_year_sum_dist
    FROM stats
    WINDOW w_n_source_search_min_last_pla AS (PARTITION BY n_source_search, min_last_played_at_year)
)
SELECT
    track_is_explicit,
    listener_country,
    n_source_search,
    track_duration_sec_vs_played_at_year_device_avg_,
    running_n_source_search,
    distinct_listener_countrys_vs_n_source_search_mi,
    n_source_search_min_last_played_at_year_sum_dist,
    ABS(median_played_at_year - 2025) AS median_played_at_year_distance,
    CASE WHEN min_last_played_at_year >= 2026 THEN 'A' WHEN min_last_played_at_year >= 2024 THEN 'B' ELSE 'C' END AS min_last_played_at_year_tier
FROM sequenced
ORDER BY listener_country, track_duration_sec_vs_played_at_year_device_avg_ DESC NULLS FIRST
