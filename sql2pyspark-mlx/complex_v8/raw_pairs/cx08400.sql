WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        UPPER(TRIM(product)) AS product_clean,
        LAST_DAY(start_date) AS start_date_month_end,
        QUARTER(start_date) AS start_date_quarter
    FROM policies
    WHERE status IS NOT NULL
      AND premium > 1042.8
),
combined_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.product_clean,
        s.start_date_month_end,
        s.start_date_quarter,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM cleaned_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        product,
        status,
        product_clean,
        start_date_month_end,
        start_date_quarter,
        DATEDIFF(DATE '2025-06-30', start_date_month_end) AS days_since_start_date_month_end
    FROM combined_details_holder
    WHERE product IS NOT NULL
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT SUM(p.premium) FROM policies p WHERE p.holder_id = s.holder_id) AS total_policy_premium,
        (SELECT COUNT(CASE WHEN p.product = 'TRAVEL' THEN 1 END) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_product_count,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count
    FROM labeled s
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.start_date_month_end,
        s.start_date_quarter,
        s.days_since_start_date_month_end,
        s.total_policy_premium,
        s.policy_product_count,
        s.policy_count,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment
    FROM profiled s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score < 0.8
),
combined_details_holder_2 AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.start_date_month_end,
        s.start_date_quarter,
        s.days_since_start_date_month_end,
        s.total_policy_premium,
        s.policy_product_count,
        s.holder_risk_score,
        s.holder_segment,
        p.holder_name,
        p.risk_score AS holder_risk_score_2,
        p.region AS holder_region
    FROM combined_details s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    status,
    start_date_month_end,
    start_date_quarter,
    days_since_start_date_month_end,
    total_policy_premium,
    policy_product_count,
    holder_name,
    holder_risk_score_2,
    holder_region,
    ADD_MONTHS(start_date_month_end, 12) AS start_date_month_end_plus_12m,
    CASE WHEN start_date_quarter >= 3 THEN 'A' WHEN start_date_quarter >= 2 THEN 'B' ELSE 'C' END AS start_date_quarter_tier
FROM combined_details_holder_2
WHERE holder_segment = 'RETAIL'
ORDER BY id, product DESC
