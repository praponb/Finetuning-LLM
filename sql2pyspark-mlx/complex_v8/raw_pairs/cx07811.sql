WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        CASE WHEN premium >= 3521.8 THEN 'large' WHEN premium >= 2033.8 THEN 'medium' ELSE 'small' END AS premium_tier,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM policies
    WHERE (holder_id >= 14 OR product IN ('HEALTH', 'LIFE'))
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        product_slug,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM recent
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(premium / 1000) * 1000 AS premium_bucket_2,
        LENGTH(TRIM(product)) AS product_len
    FROM annotated
    WHERE holder_id >= 6
),
tagged AS (
    -- Derive helper columns
    SELECT *, DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date
    FROM scored
    WHERE holder_id IS NOT NULL
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.adjuster_id >= 6
              AND c.reported_at >= TIMESTAMP '2025-01-25 11:00:00'
        )
      AND s.premium < 3544.1
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        holder_id,
        product,
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        MAX(start_date) AS last_start_date,
        MAX(end_date) AS last_end_date,
        COUNT(DISTINCT product) AS distinct_products,
        COUNT(*) AS row_count,
        MIN(premium_bucket) AS min_premium_bucket
    FROM screened
    GROUP BY holder_id, product, DATE_TRUNC('MONTH', end_date)
)
-- Final projection
SELECT
    holder_id,
    last_start_date,
    distinct_products,
    row_count,
    min_premium_bucket
FROM grouped
ORDER BY holder_id DESC
