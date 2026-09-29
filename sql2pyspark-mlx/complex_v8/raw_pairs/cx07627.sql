WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        DATE_TRUNC('YEAR', played_at) AS played_at_year
    FROM plays
    WHERE device IN ('phone', 'tv', 'speaker')
),
with_parent_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.played_at_year,
        l.birth_year AS listener_birth_year,
        l.signup_date AS listener_signup_date
    FROM scoped_plays s
    JOIN listeners l ON l.id = s.listener_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(played_at_year) OVER w_played_at_year_by_played_at AS next_played_at_year,
        ROUND(listener_birth_year / NULLIF(SUM(listener_birth_year) OVER w_track_id_is_skipped, 0), 4) AS listener_birth_year_share_of_track_id_is_skipped,
        ROUND(PERCENT_RANK() OVER w_played_at_year_month_track_i, 3) AS ms_played_percent_rank,
        COUNT(*) OVER (PARTITION BY listener_id, is_skipped) AS rows_in_listener_id_is_skipped,
        ROW_NUMBER() OVER w_played_at_year_by_played_at AS played_at_seq,
        NTILE(5) OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id) AS ms_played_quintile,
        LEAD(listener_birth_year, 2) OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id) AS next2_listener_birth_year,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id), 3) AS ms_played_percent_rank_2,
        LAST_VALUE(played_at_year) IGNORE NULLS OVER (PARTITION BY YEAR(played_at) ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_year,
        LAG(ms_played, 2) OVER (ORDER BY played_at ASC, id) AS prev2_ms_played,
        SUM(listener_birth_year) OVER w_track_id_is_skipped AS track_id_is_skipped_sum_listener_birth_year,
        ROUND(listener_birth_year - AVG(listener_birth_year) OVER (PARTITION BY track_id, is_skipped), 2) AS listener_birth_year_vs_track_id_is_skipped_avg,
        LAST_VALUE(played_at_year) IGNORE NULLS OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_year_2
    FROM with_parent_listener
    WINDOW w_played_at_year_by_played_at AS (PARTITION BY YEAR(played_at) ORDER BY played_at, id), w_track_id_is_skipped AS (PARTITION BY track_id, is_skipped), w_played_at_year_month_track_i AS (PARTITION BY DATE_TRUNC('MONTH', played_at_year), track_id ORDER BY ms_played NULLS LAST)
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE played_at_seq <= 5
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(ms_played_quintile) OVER w_last_known_played_at_year_2 AS last_known_played_at_year_2_to_date_min_ms_playe,
        NTH_VALUE(ms_played_percent_rank_2, 2) OVER (ORDER BY played_at_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_percent_rank_2,
        MIN(played_at_seq) OVER (PARTITION BY TO_DATE(last_known_played_at_year_2)) AS last_known_played_at_year_2_to_date_min_played_a,
        ROUND(CUME_DIST() OVER (ORDER BY played_at_year ASC), 3) AS played_at_year_cume_dist,
        FIRST_VALUE(ms_played_quintile) OVER (ORDER BY played_at_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played_quintile
    FROM leaders
    WINDOW w_last_known_played_at_year_2 AS (PARTITION BY TO_DATE(last_known_played_at_year_2))
),
summary AS (
    -- Summarise per group
    SELECT
        played_at_seq,
        COUNT(*) FILTER (WHERE device IN ('desktop', 'tv', 'car')) AS n_device_in_desktop_tv_car,
        SUM(listener_birth_year_share_of_track_id_is_skipped) AS total_listener_birth_year_share_of_track_id_is_s,
        MIN(ms_played_percent_rank_2) AS min_ms_played_percent_rank_2,
        DATEDIFF(MAX(listener_signup_date), MIN(listener_signup_date)) AS listener_signup_date_span_days,
        ROUND(AVG(prev2_ms_played), 2) AS avg_prev2_ms_played
    FROM metrics
    GROUP BY played_at_seq
    HAVING AVG(prev2_ms_played) > 71476
)
-- Final projection
SELECT
    played_at_seq,
    min_ms_played_percent_rank_2,
    avg_prev2_ms_played,
    CASE WHEN played_at_seq >= 7 THEN 'HIGH' WHEN played_at_seq >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS played_at_seq_tier
FROM summary
ORDER BY avg_prev2_ms_played DESC, played_at_seq
