WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
    WHERE track_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        played_at,
        source,
        ROUND(CUME_DIST() OVER (PARTITION BY source, DATE_TRUNC('MONTH', played_at) ORDER BY played_at ASC), 3) AS played_at_cume_dist,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped_label, MONTH(played_at)), 2) AS is_skipped_label_played_at_month_avg_ms_played,
        MAX(ms_played) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ms_played_max_3,
        SUM(ms_played) OVER (ORDER BY played_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        ROW_NUMBER() OVER (ORDER BY played_at DESC NULLS FIRST, id) AS seq_no,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY is_skipped_label, MONTH(played_at)), 2) AS ms_played_vs_is_skipped_label_played_at_month_av,
        LAST_VALUE(played_at) OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at
    FROM prepared_plays
),
with_windows_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        played_at,
        source,
        played_at_cume_dist,
        is_skipped_label_played_at_month_avg_ms_played,
        ms_played_max_3,
        running_ms_played,
        first_known_ms_played,
        seq_no,
        ms_played_vs_is_skipped_label_played_at_month_av,
        last_played_at,
        MIN(ms_played_max_3) OVER w_played_at_to_date_last_playe AS played_at_to_date_last_played_at_year_min_ms_pla,
        LEAD(running_ms_played, 2) OVER (ORDER BY is_skipped_label_played_at_month_avg_ms_played DESC, id) AS next2_running_ms_played,
        MAX(running_ms_played) OVER (PARTITION BY TO_DATE(played_at), YEAR(last_played_at)) AS played_at_to_date_last_played_at_year_max_runnin,
        ROUND(CUME_DIST() OVER w_all_by_is_skipped_label_play, 3) AS is_skipped_label_played_at_month_avg_ms_played_c
    FROM with_windows
    WINDOW w_played_at_to_date_last_playe AS (PARTITION BY TO_DATE(played_at), YEAR(last_played_at)), w_all_by_is_skipped_label_play AS (ORDER BY is_skipped_label_played_at_month_avg_ms_played DESC, id)
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', last_played_at) AS last_played_at_month,
        seq_no,
        source,
        SUM(ms_played_vs_is_skipped_label_played_at_month_av) AS total_ms_played_vs_is_skipped_label_played_at_mo,
        ROUND(AVG(ms_played_max_3), 2) AS avg_ms_played_max_3,
        COUNT(*) FILTER (WHERE source = 'radio') AS n_source_radio,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT source) AS distinct_sources,
        SUM(first_known_ms_played) AS total_first_known_ms_played
    FROM with_windows_2
    GROUP BY 1, 2, 3
    HAVING AVG(running_ms_played) > 404500
)
-- Final projection
SELECT
    last_played_at_month,
    source,
    total_ms_played_vs_is_skipped_label_played_at_mo,
    avg_ms_played_max_3,
    n_source_radio,
    n_rows,
    total_first_known_ms_played
FROM totals
ORDER BY total_ms_played_vs_is_skipped_label_played_at_mo DESC
