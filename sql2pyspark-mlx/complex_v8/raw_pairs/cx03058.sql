WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE is_skipped = FALSE
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        played_at,
        is_skipped,
        source,
        ROW_NUMBER() OVER (ORDER BY ms_played NULLS LAST, id) AS seq_no,
        ROUND(ms_played - AVG(ms_played) OVER w_is_skipped, 2) AS ms_played_vs_is_skipped_avg,
        ROUND(PERCENT_RANK() OVER w_all_by_ms_played, 3) AS ms_played_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY played_at), 3) AS played_at_percent_rank,
        ROW_NUMBER() OVER w_all_by_played_at AS rn,
        RANK() OVER w_all_by_played_at AS played_at_rank,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY is_skipped ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        ROUND(AVG(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_1_prior,
        ROUND(CUME_DIST() OVER w_all_by_ms_played, 3) AS ms_played_cume_dist
    FROM prepared_plays
    WINDOW w_is_skipped AS (PARTITION BY is_skipped), w_all_by_ms_played AS (ORDER BY ms_played NULLS LAST, id), w_all_by_played_at AS (ORDER BY played_at, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN source IN ('playlist', 'search') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group,
        CASE WHEN source IN ('search', 'album') THEN 'group_a' WHEN source = 'playlist' THEN 'group_b' ELSE 'other' END AS source_group_2,
        ms_played_cume_dist + ms_played_avg_1_prior AS ms_played_cume_dist_plus_ms_played_avg_1_prior
    FROM windowed
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        is_skipped,
        MIN(seq_no) AS min_seq_no,
        COUNT(DISTINCT source_group_2) AS distinct_source_group_2s
    FROM annotated
    GROUP BY is_skipped
    HAVING AVG(played_at_rank) > 4
)
-- Final projection
SELECT is_skipped, min_seq_no, distinct_source_group_2s
FROM grouped
ORDER BY distinct_source_group_2s DESC, min_seq_no NULLS LAST
