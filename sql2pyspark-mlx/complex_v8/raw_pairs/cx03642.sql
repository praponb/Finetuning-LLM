WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        DATE_TRUNC('WEEK', end_date) AS end_date_week,
        INITCAP(TRIM(status)) AS status_clean,
        ADD_MONTHS(end_date, 1) AS end_date_plus_1m
    FROM policies
    WHERE holder_id >= 2
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.*,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment
    FROM valid s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score BETWEEN 0.2 AND 0.6
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT product, holder_id, holder_segment, holder_risk_score
    FROM with_details_holder
) src
PIVOT (
    SUM(holder_risk_score)
    FOR holder_segment IN ('SME', 'RETAIL')
)
