WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        ABS(premium - 3465.4) AS premium_distance,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix
    FROM policies
    WHERE premium >= 1132.1
      AND status = 'LAPSED'
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        status,
        premium_distance,
        status_prefix,
        SUBSTRING(TRIM(status_prefix), 1, 2) AS status_prefix_prefix,
        LENGTH(TRIM(product)) AS product_len,
        ROUND(premium_bucket / NULLIF(premium, 0), 3) AS premium_bucket_per_premium
    FROM qualified
    WHERE status_prefix IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_holder_id_status_by_premium, 3) AS premium_bucket_per_premium_cume_dist,
        LAG(product_len) OVER (PARTITION BY holder_id, status ORDER BY premium_bucket_per_premium, id) AS prev_product_len,
        LAST_VALUE(premium_bucket_per_premium) OVER (PARTITION BY holder_id, status ORDER BY premium_bucket_per_premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium_bucket_per_premium,
        SUM(premium_distance) OVER (PARTITION BY status) AS status_sum_premium_distance,
        ROUND(AVG(product_len) OVER (PARTITION BY holder_id, status ORDER BY premium_bucket_per_premium, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS product_len_avg_7,
        LEAD(premium_bucket_per_premium, 2) OVER (PARTITION BY status ORDER BY product_len DESC NULLS FIRST, id) AS next2_premium_bucket_per_premium
    FROM tagged
    WINDOW w_holder_id_status_by_premium AS (PARTITION BY holder_id, status ORDER BY premium_bucket_per_premium)
)
SELECT
    product_len,
    holder_id,
    prev_product_len,
    COUNT(*) FILTER (WHERE status IN ('LAPSED', 'ACTIVE', 'EXPIRED')) AS n_status_in_lapsed_active_expi
FROM windowed
GROUP BY GROUPING SETS ((product_len, holder_id, prev_product_len), (product_len), (holder_id), ())
ORDER BY product_len NULLS LAST, holder_id NULLS LAST, prev_product_len NULLS LAST
