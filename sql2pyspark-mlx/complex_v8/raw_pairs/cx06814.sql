WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        CASE WHEN premium >= 3465.4 THEN 'HIGH' WHEN premium >= 2096.6 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE product IN ('LIFE', 'HOME')
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        QUARTER(start_date) AS start_date_quarter,
        DATE_TRUNC('WEEK', start_date) AS start_date_week,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label
    FROM scoped_policys
    WHERE status_or_unknown IS NOT NULL
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on,
        p.region AS holder_region
    FROM labeled s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Subtotals with CUBE
SELECT
    start_date_quarter,
    holder_id,
    ROUND(SUM(start_date_quarter) / NULLIF(SUM(premium), 0), 3) AS start_date_quarter_to_premium_ratio,
    MIN(start_date_quarter) AS min_start_date_quarter,
    COUNT(DISTINCT premium_tier) AS distinct_premium_tiers
FROM joined_holder
GROUP BY CUBE(start_date_quarter, holder_id)
ORDER BY start_date_quarter NULLS LAST, holder_id NULLS LAST
