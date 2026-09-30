WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date
    FROM policies
    WHERE end_date BETWEEN DATE '2025-10-12' AND DATE '2027-01-03'
      AND status IN ('ACTIVE', 'LAPSED')
),
combined_details_holder AS (
    -- Join policyholders
    SELECT
        s.*,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on
    FROM scoped s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS first_claim_date,
        (SELECT SUM(c.approved_amount) FROM claims c WHERE c.policy_id = s.id) AS total_claim_approved_amount
    FROM combined_details_holder s
),
per_group AS (
    -- Summarise per group
    SELECT
        holder_segment,
        DATE_TRUNC('MONTH', first_claim_date) AS first_claim_date_month,
        COUNT(*) FILTER (WHERE holder_segment NOT IN ('SME', 'VIP')) AS n_holder_segment_not_in_sme_vi,
        ROUND(AVG(premium), 2) AS avg_premium,
        COUNT(*) AS n_rows,
        MAX(holder_risk_score) AS max_holder_risk_score
    FROM profiled
    GROUP BY holder_segment, DATE_TRUNC('MONTH', first_claim_date)
),
scored AS (
    -- Derive helper columns
    SELECT
        holder_segment,
        first_claim_date_month,
        n_holder_segment_not_in_sme_vi,
        avg_premium,
        n_rows,
        max_holder_risk_score,
        YEAR(first_claim_date_month) AS first_claim_date_month_year,
        SUBSTRING(TRIM(holder_segment), 1, 3) AS holder_segment_prefix
    FROM per_group
    WHERE avg_premium < 4280.0
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM scored
    WHERE n_holder_segment_not_in_sme_vi > (SELECT MIN(n_holder_segment_not_in_sme_vi) FROM scored)
)
-- Final projection
SELECT
    holder_segment,
    n_holder_segment_not_in_sme_vi,
    avg_premium,
    n_rows,
    max_holder_risk_score,
    first_claim_date_month_year,
    ROUND(avg_premium * 1.5, 2) AS avg_premium_adj,
    DATE_TRUNC('WEEK', first_claim_date_month) AS first_claim_date_month_week
FROM qualified
ORDER BY n_holder_segment_not_in_sme_vi, n_rows DESC
