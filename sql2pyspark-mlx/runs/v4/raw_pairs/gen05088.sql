WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS key, v
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        created_at,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at,
        COALESCE(key, 'OTHER') AS key_clean
    FROM exploded
    WHERE created_at > DATE '2025-03-02'
),
base_carts AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        days_since_created_at,
        key_clean,
        COALESCE(days_since_created_at, 0) AS days_since_created_at_filled,
        CASE WHEN days_since_created_at >= 82 THEN 'High' WHEN days_since_created_at >= 14 THEN 'Medium' ELSE 'Low' END AS days_since_created_at_band
    FROM base
    WHERE customer_id IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        customer_id,
        days_since_created_at,
        key_clean,
        days_since_created_at_filled,
        days_since_created_at_band,
        DENSE_RANK() OVER w_grp AS days_since_created_at_dense_rank,
        RANK() OVER (PARTITION BY customer_id ORDER BY days_since_created_at DESC) AS days_since_created_at_rank,
        CUME_DIST() OVER w_running AS days_since_created_at_cume_dist
    FROM base_carts
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY days_since_created_at DESC NULLS FIRST), w_running AS (PARTITION BY customer_id ORDER BY days_since_created_at NULLS LAST)
),
scored_carts AS (
    -- Running and moving aggregates
    SELECT
        customer_id,
        days_since_created_at,
        key_clean,
        days_since_created_at_filled,
        days_since_created_at_band,
        days_since_created_at_dense_rank,
        days_since_created_at_rank,
        days_since_created_at_cume_dist,
        ROUND(days_since_created_at_rank / SUM(days_since_created_at_rank) OVER (PARTITION BY customer_id), 4) AS days_since_created_at_rank_share,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY days_since_created_at_cume_dist DESC) AS days_since_created_at_cume_dist_dense_rank,
        DENSE_RANK() OVER w_running AS days_since_created_at_dense_rank_2
    FROM scored
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY days_since_created_at NULLS LAST)
)
SELECT
    customer_id,
    days_since_created_at,
    key_clean,
    days_since_created_at_filled,
    days_since_created_at_band,
    days_since_created_at_cume_dist,
    days_since_created_at_rank_share,
    days_since_created_at_cume_dist_dense_rank,
    days_since_created_at_dense_rank_2
FROM scored_carts
ORDER BY days_since_created_at_cume_dist DESC;
