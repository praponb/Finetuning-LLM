WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        li_item.qty
    FROM carts
    LATERAL VIEW EXPLODE(items) e AS li_item
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(qty) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS qty_avg_2h,
        CUME_DIST() OVER (ORDER BY qty DESC) AS qty_cume_dist
    FROM flattened
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(qty_avg_2h) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS qty_avg_2h_avg_30m,
        RANK() OVER (ORDER BY qty_cume_dist DESC) AS qty_cume_dist_rank,
        MAX(qty_avg_2h) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS qty_avg_2h_max_15m,
        PERCENT_RANK() OVER (ORDER BY qty_cume_dist NULLS FIRST) AS qty_cume_dist_percent_rank
    FROM with_windows
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(qty_cume_dist_percent_rank) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS qty_cume_dist_percent_rank_avg_15m,
        DENSE_RANK() OVER w_ordered AS qty_cume_dist_dense_rank,
        RANK() OVER (ORDER BY qty_cume_dist_rank DESC NULLS LAST) AS qty_cume_dist_rank_rank,
        MAX(qty_cume_dist_rank) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS qty_cume_dist_rank_max_2h,
        CUME_DIST() OVER (ORDER BY qty_avg_2h_avg_30m NULLS FIRST) AS qty_avg_2h_avg_30m_cume_dist
    FROM enriched
    WINDOW w_ordered AS (ORDER BY qty_cume_dist DESC)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, qty_cume_dist_percent_rank + qty_cume_dist_rank_max_2h AS qty_cume_dist_percent_rank_plus_qty_cume_dist_rank_max_2h
    FROM scored
    WHERE created_at >= TIMESTAMP '2025-03-02 15:00:00'
),
filtered_carts AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(qty_avg_2h_avg_30m * 1.15, 2) AS adjusted_qty_avg_2h_avg_30m,
        ROUND(qty_cume_dist_dense_rank, 2) AS qty_cume_dist_dense_rank_rounded
    FROM filtered
    WHERE created_at >= DATE '2025-03-01'
),
enriched_carts AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        qty,
        qty_avg_2h,
        qty_cume_dist,
        qty_avg_2h_avg_30m,
        qty_cume_dist_rank,
        qty_avg_2h_max_15m,
        qty_cume_dist_percent_rank,
        qty_cume_dist_percent_rank_avg_15m,
        qty_cume_dist_dense_rank,
        qty_cume_dist_rank_rank,
        qty_cume_dist_rank_max_2h,
        qty_avg_2h_avg_30m_cume_dist,
        qty_cume_dist_percent_rank_plus_qty_cume_dist_rank_max_2h,
        adjusted_qty_avg_2h_avg_30m,
        qty_cume_dist_dense_rank_rounded,
        MAX(qty_cume_dist_rank) OVER w_part AS qty_cume_dist_rank_max_30m,
        COUNT(qty_cume_dist_dense_rank_rounded) OVER w_part_2 AS qty_cume_dist_dense_rank_rounded_count_2h
    FROM filtered_carts
    WINDOW w_part AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW), w_part_2 AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN qty_avg_2h_avg_30m >= 4 THEN 'LARGE' ELSE 'SMALL' END AS qty_avg_2h_avg_30m_tier,
    MIN(created_at) AS first_seen,
    MAX(created_at) AS last_created_at,
    MAX(created_at) AS ended_at
FROM enriched_carts
GROUP BY CASE WHEN qty_avg_2h_avg_30m >= 4 THEN 'LARGE' ELSE 'SMALL' END;
