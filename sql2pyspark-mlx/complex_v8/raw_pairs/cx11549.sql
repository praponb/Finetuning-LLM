WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        MONTH(played_at) AS played_at_month
    FROM plays
    WHERE played_at < TIMESTAMP '2025-03-02 11:00:00'
      AND is_skipped = TRUE
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY is_skipped) AS rows_in_is_skipped,
        COUNT(*) OVER (PARTITION BY played_at_month) AS rows_in_played_at_month,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id ORDER BY played_at_month DESC, id), 3) AS played_at_month_percent_rank,
        ROUND(AVG(played_at_month) OVER (ORDER BY played_at_month DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS played_at_month_avg_6_prior,
        ROUND(played_at_month / NULLIF(SUM(played_at_month) OVER w_is_skipped, 0), 4) AS played_at_month_share_of_is_skipped,
        ROUND(CUME_DIST() OVER w_all_by_played_at_ties, 3) AS played_at_cume_dist,
        SIZE(COLLECT_SET(track_id) OVER w_is_skipped) AS distinct_track_id_in_is_skipped,
        MAX(played_at_month) OVER w_played_at_month AS played_at_month_max_played_at_month,
        ROUND(PERCENT_RANK() OVER (ORDER BY played_at_month ASC), 3) AS played_at_month_percent_rank_2,
        ROUND(CUME_DIST() OVER (ORDER BY played_at_month DESC), 3) AS played_at_month_cume_dist,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY played_at_month)) AS distinct_listener_id_in_played_at_month
    FROM prepared_plays
    WINDOW w_is_skipped AS (PARTITION BY is_skipped), w_all_by_played_at_ties AS (ORDER BY played_at), w_played_at_month AS (PARTITION BY played_at_month)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        played_at,
        is_skipped,
        source,
        source_slug,
        rows_in_is_skipped,
        rows_in_played_at_month,
        played_at_month_avg_6_prior,
        played_at_month_share_of_is_skipped,
        played_at_month_max_played_at_month,
        played_at_month_percent_rank_2,
        played_at_month_cume_dist,
        ROUND(rows_in_is_skipped / NULLIF(SUM(rows_in_is_skipped) OVER w_is_skipped, 0), 4) AS rows_in_is_skipped_share_of_is_skipped,
        NTH_VALUE(rows_in_played_at_month, 2) OVER (PARTITION BY distinct_listener_id_in_played_at_month, listener_id ORDER BY rows_in_is_skipped DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_played_at_month,
        COUNT(*) OVER (PARTITION BY distinct_track_id_in_is_skipped, source ORDER BY rows_in_played_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        COUNT(*) OVER w_is_skipped AS rows_in_is_skipped_2
    FROM with_trends
    WINDOW w_is_skipped AS (PARTITION BY is_skipped)
)
SELECT
    id,
    listener_id,
    played_at,
    is_skipped,
    source,
    rows_in_is_skipped,
    played_at_month_avg_6_prior,
    played_at_month_share_of_is_skipped,
    played_at_month_max_played_at_month,
    played_at_month_percent_rank_2,
    rows_in_is_skipped_share_of_is_skipped,
    second_rows_in_played_at_month,
    running_count,
    rows_in_is_skipped_2
FROM metrics
WHERE MONTH(played_at) IN (1, 2)
ORDER BY rows_in_is_skipped, id
LIMIT 10
