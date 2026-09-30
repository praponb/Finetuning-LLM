WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        DATE_TRUNC('MONTH', played_at) AS played_at_month
    FROM plays
    WHERE ms_played <= 223828
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(played_at), listener_id ORDER BY played_at), 3) AS played_at_cume_dist,
        DENSE_RANK() OVER (ORDER BY played_at ASC, id) AS played_at_dense_rank,
        ROUND(AVG(ms_played) OVER (PARTITION BY TO_DATE(played_at), track_id ORDER BY played_at_month, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_2_prior,
        MIN(ms_played) OVER (PARTITION BY track_id) AS track_id_min_ms_played,
        SUM(ms_played) OVER (PARTITION BY TO_DATE(played_at), listener_id ORDER BY played_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_sum_2,
        ROUND(PERCENT_RANK() OVER w_all_by_played_at_2, 3) AS played_at_percent_rank,
        FIRST_VALUE(played_at_month) IGNORE NULLS OVER (PARTITION BY TO_DATE(played_at), track_id ORDER BY played_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_month,
        SUM(ms_played) OVER w_listener_id_source AS listener_id_source_sum_ms_played
    FROM scoped_plays
    WINDOW w_all_by_played_at_2 AS (ORDER BY played_at DESC, id), w_listener_id_source AS (PARTITION BY listener_id, source)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(played_at_dense_rank) OVER (PARTITION BY listener_id, MONTH(first_known_played_at_month) ORDER BY played_at_percent_rank DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at_dense_rank,
        FIRST_VALUE(ms_played_sum_2) IGNORE NULLS OVER (ORDER BY track_id_min_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played_sum_2,
        ROUND(ms_played_sum_2 - AVG(ms_played_sum_2) OVER (PARTITION BY YEAR(first_known_played_at_month), played_at_dense_rank), 2) AS ms_played_sum_2_vs_first_known_played_at_month_y,
        played_at_dense_rank - LAG(played_at_dense_rank) OVER (ORDER BY track_id_min_ms_played, id) AS played_at_dense_rank_change
    FROM ranked
)
-- Final projection
SELECT
    id,
    track_id,
    ms_played,
    source,
    played_at_month,
    played_at_cume_dist,
    ms_played_avg_2_prior,
    track_id_min_ms_played,
    played_at_percent_rank,
    listener_id_source_sum_ms_played,
    first_played_at_dense_rank,
    ms_played_sum_2_vs_first_known_played_at_month_y,
    track_id_min_ms_played - ms_played AS track_id_min_ms_played_minus_ms_played,
    ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(played_at_month)) / 3600, 1) AS hours_played_at_month_to_played_at
FROM sequenced
ORDER BY first_played_at_dense_rank DESC
