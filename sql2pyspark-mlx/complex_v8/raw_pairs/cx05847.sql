WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date_2
    FROM policies
    WHERE end_date IS NOT NULL
      AND start_date < DATE '2025-02-14'
),
with_parent_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.days_end_date_to_start_date,
        s.days_end_date_to_start_date_2,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment
    FROM valid s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.segment IN ('RETAIL', 'VIP')
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(status, 'ALL') AS status,
    COALESCE(holder_segment, 'ALL') AS holder_segment,
    COUNT(*) AS n_rows
FROM with_parent_holder
GROUP BY ROLLUP(status, holder_segment)
