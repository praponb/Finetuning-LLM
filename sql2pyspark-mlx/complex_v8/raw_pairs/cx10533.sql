WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.source,
        l.signup_date AS listener_signup_date,
        l.plan AS listener_plan,
        l.username AS listener_username
    FROM scoped s
    JOIN listeners l ON l.id = s.listener_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        track_id,
        ms_played,
        is_skipped,
        listener_signup_date,
        listener_username,
        COUNT(*) OVER (PARTITION BY track_id, source) AS rows_in_track_id_source,
        LEAD(ms_played) OVER (PARTITION BY MONTH(listener_signup_date), track_id ORDER BY listener_signup_date, id) AS next_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id, source), 2) AS ms_played_vs_track_id_source_avg,
        ROUND(CUME_DIST() OVER w_track_id_source_by_listener, 3) AS listener_signup_date_cume_dist,
        DENSE_RANK() OVER (PARTITION BY MONTH(listener_signup_date), track_id ORDER BY listener_signup_date, id) AS listener_signup_date_dense_rank,
        LAG(listener_signup_date) OVER (PARTITION BY track_id, source ORDER BY listener_signup_date ASC NULLS LAST, id) AS prev_listener_signup_date,
        COUNT(*) OVER (PARTITION BY is_skipped) AS rows_in_is_skipped,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY is_skipped)) AS distinct_track_id_in_is_skipped,
        SIZE(COLLECT_SET(listener_plan) OVER (PARTITION BY track_id, source)) AS distinct_listener_plan_in_track_id_source,
        RANK() OVER (PARTITION BY listener_plan, MONTH(listener_signup_date) ORDER BY listener_signup_date) AS listener_signup_date_rank,
        COUNT(*) OVER (PARTITION BY track_id, source ORDER BY listener_signup_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER w_listener_plan_listener_signu_2 AS listener_signup_date_seq,
        COUNT(*) OVER w_listener_id_track_id AS rows_in_listener_id_track_id
    FROM with_parent
    WINDOW w_track_id_source_by_listener AS (PARTITION BY track_id, source ORDER BY listener_signup_date ASC NULLS LAST), w_listener_plan_listener_signu_2 AS (PARTITION BY listener_plan, MONTH(listener_signup_date) ORDER BY listener_signup_date, id), w_listener_id_track_id AS (PARTITION BY listener_id, track_id)
),
enriched AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(listener_username)) AS listener_username_len
    FROM metrics
    WHERE next_ms_played >= 67932
),
totals AS (
    -- Group-level totals
    SELECT
        listener_signup_date_rank,
        is_skipped,
        distinct_listener_plan_in_track_id_source,
        ROUND(100 * COUNT(CASE WHEN is_skipped = FALSE THEN 1 END) / COUNT(*), 1) AS pct_is_skipped_false,
        ROUND(SUM(next_ms_played) / NULLIF(SUM(distinct_track_id_in_is_skipped), 0), 3) AS next_ms_played_to_distinct_track_id_in_is_skippe,
        COUNT(*) FILTER (WHERE is_skipped = FALSE) AS n_is_skipped_false,
        SUM(rows_in_track_id_source) AS total_rows_in_track_id_source,
        ROUND(SUM(ms_played_vs_track_id_source_avg), 2) AS total_ms_played_vs_track_id_source_avg
    FROM enriched
    WHERE listener_signup_date_seq <= 8
    GROUP BY listener_signup_date_rank, is_skipped, distinct_listener_plan_in_track_id_source
    HAVING MAX(rows_in_track_id_source) > 9
)
SELECT
    listener_signup_date_rank,
    distinct_listener_plan_in_track_id_source,
    pct_is_skipped_false,
    n_is_skipped_false,
    GREATEST(pct_is_skipped_false, n_is_skipped_false) AS greatest_pct_is_skipped_false_n_is_skipped_false,
    CASE WHEN n_is_skipped_false >= 6 THEN 'HIGH' WHEN n_is_skipped_false >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS n_is_skipped_false_tier
FROM totals
WHERE next_ms_played_to_distinct_track_id_in_is_skippe < 8.0
ORDER BY pct_is_skipped_false DESC, n_is_skipped_false_tier DESC NULLS FIRST
