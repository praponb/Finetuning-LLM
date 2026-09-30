WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        txn_type,
        txn_time,
        is_flagged,
        TRIM(merchant) AS clean_merchant
    FROM transactions
    WHERE is_flagged = TRUE
      AND merchant IS DISTINCT FROM 'Shell'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        txn_time,
        is_flagged,
        SUM(new_group_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM islands
    INTERSECT
    SELECT id
    FROM transactions
    WHERE merchant IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    INNER JOIN kept_keys k ON s.id = k.id
),
enriched AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER w_grp AS island_id_dense_rank
    FROM reconciled
    WINDOW w_grp AS (PARTITION BY DATE_TRUNC('MONTH', txn_time), txn_type ORDER BY island_id DESC)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(island_id_dense_rank) OVER w_ordered AS island_id_dense_rank_sum_90d,
        COUNT(island_id) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS island_id_count_7d,
        CUME_DIST() OVER (ORDER BY island_id_dense_rank) AS island_id_dense_rank_cume_dist,
        SUM(island_id_dense_rank) OVER w_ordered AS island_id_dense_rank_sum_90d_2
    FROM enriched
    WHERE is_flagged
    WINDOW w_ordered AS (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        island_id_dense_rank_sum_90d,
        island_id_count_7d,
        island_id_dense_rank_cume_dist,
        AVG(island_id_dense_rank_sum_90d) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS island_id_dense_rank_sum_90d_avg_90d,
        DENSE_RANK() OVER (PARTITION BY YEAR(txn_time), txn_type ORDER BY island_id_dense_rank_sum_90d DESC NULLS LAST) AS island_id_dense_rank_sum_90d_dense_rank,
        ROUND(island_id_dense_rank_sum_90d_2 / SUM(island_id_dense_rank_sum_90d_2) OVER (PARTITION BY TO_DATE(txn_time), txn_type), 4) AS island_id_dense_rank_sum_90d_2_share
    FROM scored
),
metrics AS (
    -- Ranking and gap features
    SELECT *, PERCENT_RANK() OVER (ORDER BY island_id_dense_rank_cume_dist NULLS FIRST) AS island_id_dense_rank_cume_dist_percent_rank
    FROM with_windows
)
SELECT
    id,
    island_id_count_7d,
    island_id_dense_rank_cume_dist,
    island_id_dense_rank_sum_90d_avg_90d,
    island_id_dense_rank_sum_90d_dense_rank,
    island_id_dense_rank_sum_90d_2_share,
    island_id_dense_rank_cume_dist_percent_rank
FROM metrics
ORDER BY island_id_dense_rank_cume_dist_percent_rank DESC NULLS FIRST
