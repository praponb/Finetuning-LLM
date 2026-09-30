WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.07, 2) AS premium_adj
    FROM policies
    WHERE holder_id >= 9
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_product AS rows_in_product,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product, 0), 4) AS premium_share_of_product
    FROM valid
    WINDOW w_product AS (PARTITION BY product)
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS last_claim_reported_at
    FROM sequenced s
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        product,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        SUM(premium) AS total_premium,
        ROUND(100 * COUNT(CASE WHEN product IN ('HEALTH', 'TRAVEL', 'AUTO') THEN 1 END) / COUNT(*), 1) AS pct_product_in_health_travel_aut,
        ROUND(SUM(premium) / NULLIF(SUM(premium_share_of_product), 0), 3) AS premium_to_premium_share_of_product_ratio,
        COUNT(*) AS cnt,
        COUNT(DISTINCT product) AS distinct_products,
        SUM(CASE WHEN status NOT IN ('EXPIRED', 'ACTIVE') THEN 1 ELSE 0 END) AS n_status_not_in_expired_active
    FROM profiled
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    status,
    start_date_month,
    total_premium,
    distinct_products,
    n_status_not_in_expired_active
FROM totals
ORDER BY start_date_month DESC, total_premium DESC
