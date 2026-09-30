WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        TRUNC(start_date, 'MM') AS start_date_month_start
    FROM policies
    WHERE status <> 'ACTIVE'
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(start_date_month_start, 'yyyy-MM') AS start_date_month_start_ym,
        holder_id,
        SUM(days_start_date_to_end_date) AS total_days_start_date_to_end_date,
        MIN(premium) AS min_premium,
        ROUND(AVG(days_start_date_to_end_date), 2) AS avg_days_start_date_to_end_date,
        PERCENTILE_APPROX(days_start_date_to_end_date, 0.5) AS median_days_start_date_to_end_date
    FROM prepared_policys
    GROUP BY DATE_FORMAT(start_date_month_start, 'yyyy-MM'), holder_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(min_premium - 2600.0) AS min_premium_distance,
        ABS(min_premium - 2360.0) AS min_premium_distance_2
    FROM grouped
),
line_level AS (
    -- One row per policy
    SELECT
        s.start_date_month_start_ym,
        s.holder_id,
        s.min_premium,
        s.avg_days_start_date_to_end_date,
        s.median_days_start_date_to_end_date,
        p.id AS policy_id,
        p.premium AS policy_premium,
        p.product AS policy_product,
        p.start_date AS policy_start_date,
        p.status AS policy_status
    FROM tagged s
    LEFT JOIN policies p ON p.holder_id = s.holder_id AND p.premium >= 1042.8
),
with_activity AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM line_level s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status IN ('EXPIRED', 'ACTIVE', 'LAPSED')
              AND p.start_date >= DATE '2025-01-05'
        )
      AND s.policy_start_date < DATE '2025-02-14'
)
-- Final projection
SELECT
    start_date_month_start_ym,
    holder_id,
    median_days_start_date_to_end_date,
    policy_id,
    policy_premium,
    policy_product,
    policy_start_date,
    policy_status,
    CONCAT(policy_product, '#', CAST(policy_id AS STRING)) AS policy_product_ref,
    ROUND(min_premium * 1.07, 2) AS min_premium_adj
FROM with_activity
