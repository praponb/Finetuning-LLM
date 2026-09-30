WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status
    FROM policies
    WHERE (premium >= 2289.0 OR product IN ('LIFE', 'HEALTH'))
),
per_group AS (
    -- Group-level totals
    SELECT
        product,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        ROUND(100 * COUNT(CASE WHEN status = 'LAPSED' THEN 1 END) / COUNT(*), 1) AS pct_status_lapsed,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        COUNT(*) FILTER (WHERE product IN ('HEALTH', 'HOME', 'LIFE')) AS n_product_in_health_home_life,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        COUNT(*) FILTER (WHERE status NOT IN ('EXPIRED', 'ACTIVE')) AS n_status_not_in_expired_active
    FROM filtered_policys
    WHERE product = 'AUTO'
    GROUP BY 1, 2
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ABS(pct_status_lapsed - 65.0) AS pct_status_lapsed_distance,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM per_group
    WHERE n_status_not_in_expired_active > 2
)
-- Final projection
SELECT
    product,
    start_date_month,
    pct_status_lapsed,
    n_status_not_in_expired_active,
    product_or_unknown,
    product_slug
FROM labeled
ORDER BY product_or_unknown DESC, product_slug DESC, product, start_date_month
LIMIT 20
