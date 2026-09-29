WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
    WHERE track_id <= 24
),
joined AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        l.birth_year AS listener_birth_year,
        t.release_date AS track_release_date
    FROM valid s
    JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
    WHERE l.birth_year <= 1981
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        listener_birth_year,
        SIZE(COLLECT_SET(listener_id) OVER w_track_id) AS distinct_listener_id_in_track_id,
        MIN(listener_birth_year) OVER (PARTITION BY YEAR(played_at) ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_listener_birth_year_so_far,
        ROUND(PERCENT_RANK() OVER w_played_at_year_by_ms_played_2, 3) AS ms_played_percent_rank,
        MIN(listener_birth_year) OVER (PARTITION BY listener_id, DATE_TRUNC('MONTH', played_at)) AS listener_id_played_at_month_min_listener_birth_y,
        DENSE_RANK() OVER w_track_release_date_year_by_l AS listener_birth_year_dense_rank,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY source, listener_id)) AS distinct_track_id_in_source_listener_id,
        LAG(played_at, 2) OVER (ORDER BY listener_birth_year, id) AS prev2_played_at
    FROM joined
    WINDOW w_track_id AS (PARTITION BY track_id), w_played_at_year_by_ms_played_2 AS (PARTITION BY YEAR(played_at) ORDER BY ms_played ASC), w_track_release_date_year_by_l AS (PARTITION BY YEAR(track_release_date) ORDER BY listener_birth_year DESC, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        listener_birth_year,
        distinct_listener_id_in_track_id,
        min_listener_birth_year_so_far,
        ms_played_percent_rank,
        listener_id_played_at_month_min_listener_birth_y,
        distinct_track_id_in_source_listener_id,
        prev2_played_at
    FROM metrics
    WHERE listener_birth_year_dense_rank = 1
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        track_id,
        played_at,
        device,
        source,
        distinct_listener_id_in_track_id,
        min_listener_birth_year_so_far,
        ms_played_percent_rank,
        listener_id_played_at_month_min_listener_birth_y,
        distinct_track_id_in_source_listener_id,
        SUM(listener_birth_year) OVER (PARTITION BY source, distinct_track_id_in_source_listener_id) AS source_distinct_track_id_in_source_listener_id_s,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_listener_id_in_track_id ORDER BY played_at DESC, id), 3) AS played_at_percent_rank,
        LAG(listener_birth_year, 3, 0) OVER (PARTITION BY distinct_listener_id_in_track_id ORDER BY played_at DESC, id) AS prev3_listener_birth_year,
        SIZE(COLLECT_SET(track_id) OVER w_source_distinct_track_id_in) AS distinct_track_id_in_source_distinct_track_id_in,
        ROUND(AVG(ms_played_percent_rank) OVER (PARTITION BY distinct_listener_id_in_track_id ORDER BY played_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_percent_rank_avg_2_prior,
        COUNT(*) OVER (PARTITION BY source, distinct_track_id_in_source_listener_id) AS rows_in_source_distinct_track_id_in_source_liste
    FROM leaders
    WINDOW w_source_distinct_track_id_in AS (PARTITION BY source, distinct_track_id_in_source_listener_id)
),
stats AS (
    -- Group-level totals
    SELECT
        TO_DATE(played_at) AS played_at_day,
        device,
        MAX(min_listener_birth_year_so_far) AS max_min_listener_birth_year_so_far,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        ROUND(AVG(ms_played_percent_rank_avg_2_prior), 2) AS avg_ms_played_percent_rank_avg_2_prior
    FROM sequenced
    GROUP BY 1, 2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(played_at_day), device ORDER BY avg_ms_played_percent_rank_avg_2_prior, played_at_day, device), 3) AS avg_ms_played_percent_rank_avg_2_prior_cume_dist,
        LAG(played_at_day, 3) OVER (PARTITION BY MONTH(played_at_day), device ORDER BY avg_ms_played_percent_rank_avg_2_prior, played_at_day, device) AS prev3_played_at_day,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(played_at_day), device ORDER BY avg_ms_played_percent_rank_avg_2_prior), 3) AS avg_ms_played_percent_rank_avg_2_prior_percent_r
    FROM stats
)
-- Final projection
SELECT
    played_at_day,
    device,
    max_min_listener_birth_year_so_far,
    source_list,
    avg_ms_played_percent_rank_avg_2_prior,
    avg_ms_played_percent_rank_avg_2_prior_cume_dist,
    prev3_played_at_day,
    CASE WHEN device IN ('desktop', 'tv') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group
FROM with_windows
WHERE SIZE(source_list) >= 1
ORDER BY prev3_played_at_day, device_group
