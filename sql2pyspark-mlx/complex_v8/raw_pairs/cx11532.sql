WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug_2
    FROM policies
    WHERE holder_id IS NOT NULL
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.end_date) FROM policies p WHERE p.holder_id = s.holder_id) AS last_policy_end_date,
        (SELECT COUNT(*) FILTER (WHERE p.status = 'EXPIRED') FROM policies p WHERE p.holder_id = s.holder_id) AS policy_status_count,
        (SELECT COUNT(*) FILTER (WHERE p.product IS NOT NULL) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_product_count
    FROM filtered_policys s
),
with_activity AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_counts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status IS NOT NULL
        )
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    product,
    policy_product_count,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list
FROM with_activity
GROUP BY GROUPING SETS ((holder_id, product, policy_product_count), (holder_id))
ORDER BY holder_id NULLS LAST, product NULLS LAST, policy_product_count NULLS LAST
