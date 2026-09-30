WITH prepared_plays AS (
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
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        INITCAP(TRIM(source)) AS source_clean
    FROM plays
    WHERE ms_played >= 73335
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared_plays
    WHERE ms_played > (SELECT AVG(ms_played) FROM prepared_plays)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        source,
        is_skipped,
        COUNT(*) AS cnt,
        SUM(ms_played) AS total_ms_played,
        COUNT(DISTINCT device) AS distinct_devices,
        MAX(ms_played) AS max_ms_played
    FROM without_matches
    GROUP BY 1, 2
    HAVING MAX(ms_played) > 129336
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE cnt > (SELECT AVG(cnt) FROM rollup_base)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        source,
        is_skipped,
        cnt,
        max_ms_played,
        RANK() OVER w_distinct_devices_by_max_ms_p AS max_ms_played_rank,
        MAX(cnt) OVER (PARTITION BY is_skipped, source) AS is_skipped_source_max_cnt,
        ROUND(AVG(max_ms_played) OVER (PARTITION BY is_skipped, source), 2) AS is_skipped_source_avg_max_ms_played,
        DENSE_RANK() OVER w_cnt_is_skipped_by_distinct_d AS distinct_devices_dense_rank
    FROM with_activity
    WINDOW w_distinct_devices_by_max_ms_p AS (PARTITION BY distinct_devices ORDER BY max_ms_played, source, is_skipped), w_cnt_is_skipped_by_distinct_d AS (PARTITION BY cnt, is_skipped ORDER BY distinct_devices DESC NULLS FIRST, source, is_skipped)
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY cnt, max_ms_played_rank ORDER BY is_skipped_source_avg_max_ms_played DESC, source, is_skipped) AS is_skipped_source_avg_max_ms_played_decile,
        DENSE_RANK() OVER (PARTITION BY cnt, max_ms_played_rank ORDER BY max_ms_played) AS max_ms_played_dense_rank,
        MAX(max_ms_played) OVER (PARTITION BY cnt, max_ms_played_rank ORDER BY max_ms_played, source, is_skipped ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS max_ms_played_max_2_prior,
        ROUND(CUME_DIST() OVER w_cnt_max_ms_played_rank_by_ma, 3) AS max_ms_played_cume_dist,
        RANK() OVER (PARTITION BY cnt, max_ms_played_rank ORDER BY max_ms_played) AS max_ms_played_rank_2,
        ROUND(is_skipped_source_max_cnt / NULLIF(SUM(is_skipped_source_max_cnt) OVER w_is_skipped_max_ms_played_ran, 0), 4) AS is_skipped_source_max_cnt_share_of_is_skipped_ma
    FROM windowed
    WINDOW w_cnt_max_ms_played_rank_by_ma AS (PARTITION BY cnt, max_ms_played_rank ORDER BY max_ms_played), w_is_skipped_max_ms_played_ran AS (PARTITION BY is_skipped, max_ms_played_rank)
)
-- Subtotals with ROLLUP
SELECT
    max_ms_played_dense_rank,
    is_skipped,
    max_ms_played_rank_2,
    ROUND(AVG(max_ms_played_cume_dist), 2) AS avg_max_ms_played_cume_dist,
    COUNT(*) AS cnt_2,
    COUNT(DISTINCT source) AS distinct_sources
FROM windowed_2
GROUP BY ROLLUP(max_ms_played_dense_rank, is_skipped, max_ms_played_rank_2)
ORDER BY max_ms_played_dense_rank NULLS LAST, is_skipped NULLS LAST, max_ms_played_rank_2 NULLS LAST
