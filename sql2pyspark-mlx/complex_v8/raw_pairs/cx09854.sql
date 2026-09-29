WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date
    FROM policies
    WHERE holder_id IS NOT NULL
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        product,
        holder_id,
        MIN(premium) AS min_premium,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        SUM(CASE WHEN product = 'TRAVEL' THEN 1 ELSE 0 END) AS n_product_travel,
        COUNT(DISTINCT id) AS distinct_ids
    FROM scoped_policys
    GROUP BY product, holder_id
),
detail_rows AS (
    -- One row per policy
    SELECT
        s.product,
        s.holder_id,
        s.cnt,
        s.n_product_travel,
        s.distinct_ids,
        p.id AS policy_id,
        p.product AS policy_product,
        p.start_date AS policy_start_date,
        p.status AS policy_status
    FROM stats s
    LEFT JOIN policies p ON p.holder_id = s.holder_id AND p.status IS NOT NULL
)
-- Final projection
SELECT
    product,
    cnt,
    n_product_travel,
    distinct_ids,
    policy_product,
    policy_status
FROM detail_rows
WHERE policy_start_date < DATE '2025-05-28'
ORDER BY policy_status, policy_product DESC
