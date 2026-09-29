WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        ROUND(premium * 1.5, 2) AS premium_adj,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        INITCAP(TRIM(product)) AS product_clean
    FROM policies
    WHERE end_date BETWEEN DATE '2025-04-17' AND DATE '2026-12-05'
),
filtered AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.product = 'TRAVEL'
        )
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS first_claim_reported_at
    FROM filtered s
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        product,
        COUNT(*) AS row_count,
        ROUND(AVG(premium), 2) AS avg_premium,
        SUM(premium) FILTER (WHERE status IS NOT NULL) AS premium_status_is_not_null,
        MIN(start_date) AS first_start_date,
        MIN(premium) AS min_premium,
        MAX(first_claim_reported_at) AS last_first_claim_reported_at
    FROM with_stats
    GROUP BY DATE_TRUNC('MONTH', start_date), product
    HAVING COUNT(*) >= 2
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN avg_premium >= 3560.0 THEN 'A' WHEN avg_premium >= 1880.0 THEN 'B' ELSE 'C' END AS avg_premium_tier
    FROM totals
)
SELECT
    start_date_month,
    product,
    row_count,
    avg_premium,
    premium_status_is_not_null,
    first_start_date,
    min_premium,
    last_first_claim_reported_at,
    avg_premium_tier,
    ROUND(avg_premium * 1.07, 2) AS avg_premium_adj,
    FLOOR(row_count / 2) * 2 AS row_count_bucket
FROM annotated
ORDER BY product DESC NULLS FIRST, avg_premium_adj NULLS LAST
