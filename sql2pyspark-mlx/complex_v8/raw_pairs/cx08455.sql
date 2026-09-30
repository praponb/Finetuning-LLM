WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
    WHERE source = 'search'
),
enriched AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source_or_unknown,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title,
        t.release_date AS track_release_date,
        l.signup_date AS listener_signup_date
    FROM scoped_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
    JOIN listeners l ON l.id = s.listener_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id ORDER BY track_duration_sec DESC, id), 3) AS track_duration_sec_percent_rank,
        LEAD(track_duration_sec, 3) OVER w_is_skipped_by_track_duration AS next3_track_duration_sec,
        SUM(track_duration_sec) OVER (PARTITION BY is_skipped ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_track_duration_sec,
        NTILE(3) OVER (PARTITION BY is_skipped ORDER BY ms_played ASC, id) AS ms_played_tercile,
        MAX(ms_played) OVER w_played_at_month_source_or_un AS played_at_month_source_or_unknown_max_ms_played,
        ROUND(track_duration_sec - AVG(track_duration_sec) OVER w_listener_signup_date_month_i, 2) AS track_duration_sec_vs_listener_signup_date_month,
        MAX(ms_played) OVER w_source_or_unknown_is_skipped AS source_or_unknown_is_skipped_max_ms_played,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, source_or_unknown ORDER BY ms_played DESC, id), 3) AS ms_played_cume_dist,
        DENSE_RANK() OVER (PARTITION BY source_or_unknown ORDER BY ms_played) AS ms_played_dense_rank,
        RANK() OVER (PARTITION BY source_or_unknown ORDER BY ms_played) AS ms_played_rank,
        SIZE(COLLECT_SET(track_id) OVER w_source_or_unknown_is_skipped) AS distinct_track_id_in_source_or_unknown_is_skippe,
        ROUND(ms_played - AVG(ms_played) OVER w_listener_signup_date_month_i, 2) AS ms_played_vs_listener_signup_date_month_is_skipp,
        NTILE(3) OVER w_source_or_unknown_by_ms_play_2 AS ms_played_tercile_2,
        COUNT(*) OVER w_played_at_month_source_or_un AS rows_in_played_at_month_source_or_unknown,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY MONTH(played_at), source_or_unknown), 0), 4) AS ms_played_share_of_played_at_month_source_or_unk,
        DENSE_RANK() OVER w_all_by_played_at_ties AS played_at_dense_rank
    FROM enriched
    WINDOW w_is_skipped_by_track_duration AS (PARTITION BY is_skipped ORDER BY track_duration_sec ASC, id), w_played_at_month_source_or_un AS (PARTITION BY MONTH(played_at), source_or_unknown), w_listener_signup_date_month_i AS (PARTITION BY MONTH(listener_signup_date), is_skipped), w_source_or_unknown_is_skipped AS (PARTITION BY source_or_unknown, is_skipped), w_source_or_unknown_by_ms_play_2 AS (PARTITION BY source_or_unknown ORDER BY ms_played, id), w_all_by_played_at_ties AS (ORDER BY played_at)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        ms_played_rank,
        is_skipped,
        COUNT(*) FILTER (WHERE LOWER(track_title) LIKE '%gh%') AS n_lower_track_title_like_gh,
        ROUND(SUM(track_duration_sec_vs_listener_signup_date_month), 2) AS total_track_duration_sec_vs_listener_signup_date,
        COUNT(*) AS n_rows
    FROM with_trends
    GROUP BY ms_played_rank, is_skipped
)
-- Final projection
SELECT
    ms_played_rank,
    is_skipped,
    n_lower_track_title_like_gh,
    total_track_duration_sec_vs_listener_signup_date,
    n_rows,
    ROUND(ms_played_rank / NULLIF(n_rows, 0), 3) AS ms_played_rank_per_n_rows,
    GREATEST(ms_played_rank, n_rows) AS greatest_ms_played_rank_n_rows
FROM stats
ORDER BY n_rows
