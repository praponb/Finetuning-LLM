WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix
    FROM plays
    WHERE HOUR(played_at) BETWEEN 6 AND 9
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.played_at < TIMESTAMP '2025-02-24 12:00:00'
        )
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE NOT p.is_skipped) FROM plays p WHERE p.track_id = s.track_id) AS play_is_skipped_count,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS max_play_ms_played,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS min_play_ms_played
    FROM without_matches s
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        MIN(played_at) AS first_played_at,
        MAX(played_at) AS last_played_at
    FROM profiled
    WHERE played_at >= TIMESTAMP '2025-01-31 09:00:00'
    GROUP BY DATE_TRUNC('MONTH', played_at)
    HAVING MAX(play_is_skipped_count) > 1
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_first_played_at_month_by_fir AS seq_no,
        LAST_VALUE(played_at_month) IGNORE NULLS OVER (PARTITION BY MONTH(first_played_at), YEAR(played_at_month) ORDER BY last_played_at ASC, played_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_month,
        RANK() OVER (PARTITION BY MONTH(first_played_at), YEAR(played_at_month) ORDER BY last_played_at ASC) AS last_played_at_rank,
        NTILE(5) OVER (ORDER BY first_played_at DESC, played_at_month) AS first_played_at_quintile,
        NTH_VALUE(played_at_month, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', first_played_at) ORDER BY first_played_at ASC, played_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_month,
        NTILE(4) OVER (PARTITION BY MONTH(first_played_at), YEAR(played_at_month) ORDER BY last_played_at ASC, played_at_month) AS last_played_at_quartile
    FROM summary
    WINDOW w_first_played_at_month_by_fir AS (PARTITION BY DATE_TRUNC('MONTH', first_played_at) ORDER BY first_played_at ASC, played_at_month)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM ranked
    WHERE last_played_at_rank > (SELECT AVG(last_played_at_rank) FROM ranked)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY last_played_at_rank, TO_DATE(last_known_played_at_month) ORDER BY seq_no, played_at_month) AS seq_no_quartile,
        MIN(last_played_at_rank) OVER (PARTITION BY MONTH(second_played_at_month)) AS second_played_at_month_month_min_last_played_at_,
        MAX(last_played_at_quartile) OVER (PARTITION BY MONTH(second_played_at_month)) AS second_played_at_month_month_max_last_played_at_,
        DENSE_RANK() OVER w_all_by_last_played_at_ties AS last_played_at_dense_rank,
        ROW_NUMBER() OVER w_last_played_at_quartile_last AS last_played_at_seq
    FROM screened
    WINDOW w_all_by_last_played_at_ties AS (ORDER BY last_played_at ASC), w_last_played_at_quartile_last AS (PARTITION BY last_played_at_quartile, YEAR(last_known_played_at_month) ORDER BY last_played_at, played_at_month)
)
-- Native SQL UNPIVOT (wide to long)
SELECT played_at_month, metric, metric_value
FROM (
    SELECT played_at_month, seq_no_quartile, last_played_at_seq
    FROM with_windows
) src
UNPIVOT (
    metric_value FOR metric IN (seq_no_quartile, last_played_at_seq)
) unp
