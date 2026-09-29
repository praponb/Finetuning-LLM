WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM policies
    WHERE product <> 'HEALTH'
),
with_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status_prefix,
        p.segment AS holder_segment
    FROM filtered_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(premium) OVER (PARTITION BY YEAR(end_date)) AS end_date_year_min_premium,
        DATEDIFF(end_date, LAG(end_date) OVER w_start_date_month_by_premium) AS days_since_prev_end_date,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY MONTH(start_date) ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        FIRST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY MONTH(start_date) ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium
    FROM with_details
    WINDOW w_start_date_month_by_premium AS (PARTITION BY MONTH(start_date) ORDER BY premium DESC, id)
),
with_parent_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.holder_segment,
        s.end_date_year_min_premium,
        s.days_since_prev_end_date,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM with_trends s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score > 0.2
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, p.region AS holder_region, p.holder_name
    FROM with_parent_holder s
    JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM with_parent s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
        )
)
SELECT *
FROM (
    SELECT holder_id, product, premium
    FROM screened
) src
PIVOT (
    MIN(premium)
    FOR product IN ('LIFE', 'HEALTH', 'HOME', 'TRAVEL')
)
