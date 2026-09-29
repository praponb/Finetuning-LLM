WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix,
        CASE WHEN product IN ('HEALTH', 'TRAVEL') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE status = 'CANCELLED'
      AND product = 'HOME'
),
joined AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status,
        s.product_prefix,
        s.product_group,
        p.segment AS holder_segment
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (ORDER BY start_date ASC) AS start_date_rank,
        COUNT(*) OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        SUM(premium) OVER (PARTITION BY holder_id, holder_segment) AS holder_id_holder_segment_sum_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id, holder_segment), 2) AS premium_vs_holder_id_holder_segment_avg,
        COUNT(*) OVER w_holder_id_holder_segment AS rows_in_holder_id_holder_segment,
        MIN(premium) OVER (PARTITION BY holder_id, holder_segment) AS holder_id_holder_segment_min_premium,
        ROUND(AVG(premium) OVER (ORDER BY start_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_6_prior,
        ROW_NUMBER() OVER w_all_by_start_date AS start_date_seq,
        ROUND(premium / NULLIF(SUM(premium) OVER w_holder_id_holder_segment, 0), 4) AS premium_share_of_holder_id_holder_segment,
        RANK() OVER (ORDER BY premium) AS premium_rank,
        NTILE(10) OVER w_all_by_premium AS premium_decile,
        premium - LAG(premium) OVER w_all_by_start_date AS premium_change,
        NTILE(3) OVER (ORDER BY start_date ASC, id) AS start_date_tercile
    FROM joined
    WINDOW w_holder_id_holder_segment AS (PARTITION BY holder_id, holder_segment), w_all_by_start_date AS (ORDER BY start_date ASC, id), w_all_by_premium AS (ORDER BY premium, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE premium_rank = 1
      AND premium_change < 1500.0
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        MAX(last_known_start_date) AS last_last_known_start_date,
        COUNT(*) AS cnt,
        SUM(premium_decile) FILTER (WHERE status IS NOT NULL) AS premium_decile_status_is_not_null
    FROM top_per_group
    WHERE start_date_seq BETWEEN 2 AND 7
    GROUP BY status
    HAVING COUNT(*) >= 3
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        status,
        cnt,
        premium_decile_status_is_not_null,
        ROUND(CUME_DIST() OVER (PARTITION BY status ORDER BY premium_decile_status_is_not_null, status), 3) AS premium_decile_status_is_not_null_cume_dist,
        FIRST_VALUE(cnt) IGNORE NULLS OVER (PARTITION BY status ORDER BY premium_decile_status_is_not_null, status ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cnt
    FROM grouped
)
-- Final projection
SELECT
    status,
    cnt,
    premium_decile_status_is_not_null_cume_dist,
    first_known_cnt
FROM metrics
WHERE premium_decile_status_is_not_null >= 10
ORDER BY first_known_cnt NULLS LAST
