WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date
    FROM policies
    WHERE premium >= 2319.8
),
combined_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        p.segment AS holder_segment,
        p.holder_name,
        p.region AS holder_region
    FROM scoped_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM combined_details_holder s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND YEAR(c.claim_date) = 2024
        )
      AND s.end_date < DATE '2026-09-19'
),
with_parent_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.holder_segment,
        s.holder_name,
        s.holder_region,
        p.segment AS holder_segment_2,
        p.risk_score AS holder_risk_score
    FROM screened s
    JOIN policyholders p ON p.id = s.holder_id
),
summary AS (
    -- Summarise per group
    SELECT
        holder_segment_2,
        MIN(holder_risk_score) AS min_holder_risk_score,
        DATEDIFF(MAX(end_date), MIN(end_date)) AS end_date_span_days
    FROM with_parent_holder
    WHERE holder_segment IN ('RETAIL', 'SME')
    GROUP BY 1
)
-- Final projection
SELECT
    holder_segment_2,
    end_date_span_days,
    ROUND(min_holder_risk_score / NULLIF(end_date_span_days, 0), 3) AS min_holder_risk_score_per_end_date_span_days,
    LENGTH(TRIM(holder_segment_2)) AS holder_segment_2_len
FROM summary
ORDER BY holder_segment_2, holder_segment_2_len DESC
