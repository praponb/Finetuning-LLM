WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        DATE_TRUNC('QUARTER', end_date) AS end_date_quarter
    FROM policies
    WHERE status IS NOT NULL
      AND holder_id IS NOT NULL
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status,
        s.end_date_quarter,
        p.segment AS holder_segment
    FROM valid_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment_2,
        p.region AS holder_region
    FROM combined_details_holder s
    JOIN policyholders p ON p.id = s.holder_id AND p.region IS NOT NULL
    WHERE p.risk_score BETWEEN 0.1 AND 0.9
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('DAY', end_date_quarter) AS end_date_quarter_day,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date
    FROM with_details
    WHERE start_date < DATE '2025-08-03'
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    start_date,
    end_date_quarter,
    holder_risk_score,
    holder_segment_2,
    holder_region,
    end_date_quarter_day,
    days_since_start_date,
    CASE WHEN holder_segment IN ('VIP', 'RETAIL') THEN 'group_a' WHEN holder_segment = 'SME' THEN 'group_b' ELSE 'other' END AS holder_segment_group,
    CASE WHEN premium >= 2945.9 THEN 'A' WHEN premium >= 2096.6 THEN 'B' ELSE 'C' END AS premium_tier
FROM labeled
WHERE end_date_quarter < TIMESTAMP '2025-02-05 09:00:00'
ORDER BY holder_segment_2, id DESC NULLS FIRST
LIMIT 5
