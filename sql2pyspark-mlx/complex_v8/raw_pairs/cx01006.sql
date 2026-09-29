WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label
    FROM plays
    WHERE played_at < TIMESTAMP '2025-03-02 09:00:00'
      AND source NOT IN ('playlist', 'radio')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        played_at,
        is_skipped,
        played_at_label,
        DENSE_RANK() OVER (ORDER BY ms_played, id) AS ms_played_dense_rank,
        ms_played - LAG(ms_played) OVER w_all_by_ms_played AS ms_played_change,
        NTH_VALUE(ms_played, 2) OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        LAST_VALUE(played_at) OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        RANK() OVER w_all_by_ms_played AS ms_played_rank,
        LAG(played_at) OVER (ORDER BY ms_played, id) AS prev_played_at
    FROM cleaned_plays
    WINDOW w_all_by_ms_played AS (ORDER BY ms_played, id)
),
latest_per_key AS (
    -- Keep the latest row per track_id
    SELECT
        id,
        track_id,
        played_at,
        is_skipped,
        played_at_label,
        ms_played_dense_rank,
        ms_played_change,
        second_ms_played,
        last_played_at,
        ms_played_rank,
        prev_played_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY ms_played_dense_rank DESC, id DESC) AS recency_rank
        FROM sequenced
    ) ranked
    WHERE recency_rank = 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        played_at,
        played_at_label,
        ms_played_dense_rank,
        ms_played_change,
        last_played_at,
        ms_played_rank,
        prev_played_at,
        COUNT(*) OVER w_ms_played_dense_rank AS rows_in_ms_played_dense_rank,
        ROUND(ms_played_change / NULLIF(SUM(ms_played_change) OVER (PARTITION BY ms_played_dense_rank), 0), 4) AS ms_played_change_share_of_ms_played_dense_rank,
        DENSE_RANK() OVER (ORDER BY ms_played_change DESC) AS ms_played_change_dense_rank,
        ROUND(ms_played_change - AVG(ms_played_change) OVER (PARTITION BY ms_played_dense_rank), 2) AS ms_played_change_vs_ms_played_dense_rank_avg,
        COUNT(*) OVER (ORDER BY second_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM latest_per_key
    WINDOW w_ms_played_dense_rank AS (PARTITION BY ms_played_dense_rank)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, ms_played_change_vs_ms_played_dense_rank_avg, ms_played_change_share_of_ms_played_dense_rank
    FROM windowed
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric IN (ms_played_change_vs_ms_played_dense_rank_avg, ms_played_change_share_of_ms_played_dense_rank)
) unp
