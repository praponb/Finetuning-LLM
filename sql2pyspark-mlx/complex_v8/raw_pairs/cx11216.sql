WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_type IS NOT NULL
              AND c.approved_amount >= 0.0
        )
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY YEAR(end_date)) AS rows_in_end_date_year,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY YEAR(end_date))) AS distinct_product_in_end_date_year,
        DATEDIFF(end_date, LAG(end_date) OVER (ORDER BY start_date, id)) AS days_since_prev_end_date
    FROM qualified
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        rows_in_end_date_year,
        distinct_product_in_end_date_year,
        LAST_DAY(end_date) AS end_date_month_end,
        ROUND(premium * 0.25, 2) AS premium_adj
    FROM windowed
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.rows_in_end_date_year,
        s.distinct_product_in_end_date_year,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM scored s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    rows_in_end_date_year,
    distinct_product_in_end_date_year,
    holder_segment,
    holder_risk_score,
    holder_region,
    GREATEST(holder_risk_score, rows_in_end_date_year) AS greatest_holder_risk_score_rows_in_end_date_year,
    FLOOR(rows_in_end_date_year / 5) * 5 AS rows_in_end_date_year_bucket
FROM combined_details_holder
