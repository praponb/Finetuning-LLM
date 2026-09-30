WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix
    FROM plays
    WHERE ms_played <= 257789
      AND track_id IS NOT NULL
),
enriched_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source,
        s.device_prefix,
        l.country AS listener_country,
        l.birth_year AS listener_birth_year,
        l.plan AS listener_plan
    FROM base s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        track_id,
        device,
        device_prefix,
        listener_birth_year,
        listener_plan,
        SUM(listener_birth_year) OVER (PARTITION BY track_id) AS track_id_sum_listener_birth_year,
        SUM(listener_birth_year) OVER (PARTITION BY listener_country) AS listener_country_sum_listener_birth_year,
        DENSE_RANK() OVER (PARTITION BY source, listener_id ORDER BY played_at DESC) AS played_at_dense_rank,
        MAX(listener_birth_year) OVER (PARTITION BY track_id) AS track_id_max_listener_birth_year,
        MAX(listener_birth_year) OVER (ORDER BY played_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS listener_birth_year_max_3_prior,
        ROUND(CUME_DIST() OVER w_listener_country_by_listener, 3) AS listener_birth_year_cume_dist,
        ROUND(CUME_DIST() OVER w_all_by_listener_birth_year_t, 3) AS listener_birth_year_cume_dist_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY listener_birth_year DESC), 3) AS listener_birth_year_percent_rank,
        SUM(listener_birth_year) OVER (PARTITION BY source, listener_id ORDER BY played_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS listener_birth_year_sum_2_prior,
        listener_birth_year - LAG(listener_birth_year) OVER w_listener_country_by_listener_2 AS listener_birth_year_change,
        NTH_VALUE(listener_birth_year, 2) OVER (PARTITION BY listener_country ORDER BY listener_birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_listener_birth_year
    FROM enriched_listener
    WINDOW w_listener_country_by_listener AS (PARTITION BY listener_country ORDER BY listener_birth_year), w_all_by_listener_birth_year_t AS (ORDER BY listener_birth_year DESC), w_listener_country_by_listener_2 AS (PARTITION BY listener_country ORDER BY listener_birth_year, id)
),
top_rows AS (
    -- Top 3 per group
    SELECT
        id,
        track_id,
        device,
        device_prefix,
        listener_birth_year,
        listener_plan,
        track_id_sum_listener_birth_year,
        listener_country_sum_listener_birth_year,
        track_id_max_listener_birth_year,
        listener_birth_year_max_3_prior,
        listener_birth_year_cume_dist,
        listener_birth_year_cume_dist_2,
        listener_birth_year_percent_rank,
        listener_birth_year_sum_2_prior,
        listener_birth_year_change,
        second_listener_birth_year
    FROM with_trends
    WHERE played_at_dense_rank <= 3
)
-- Final projection
SELECT
    id,
    track_id,
    device,
    device_prefix,
    listener_birth_year,
    listener_plan,
    track_id_sum_listener_birth_year,
    track_id_max_listener_birth_year,
    listener_birth_year_max_3_prior,
    listener_birth_year_cume_dist,
    listener_birth_year_cume_dist_2,
    listener_birth_year_percent_rank,
    listener_birth_year_sum_2_prior,
    listener_birth_year_change,
    second_listener_birth_year,
    CONCAT_WS('|', device_prefix, device) AS device_prefix_device_label
FROM top_rows
WHERE listener_birth_year_percent_rank >= 0.1
ORDER BY track_id_max_listener_birth_year
