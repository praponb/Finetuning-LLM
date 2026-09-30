WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status
    FROM policies
    WHERE start_date < DATE '2025-03-21'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATE_ADD(start_date, 30) AS start_date_plus_30d,
        DAYOFWEEK(start_date) AS start_date_dayofweek
    FROM base_policys
    WHERE start_date BETWEEN DATE '2024-11-20' AND DATE '2025-03-23'
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.start_date_plus_30d,
        s.start_date_dayofweek,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM scored s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.region <> 'South'
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(status, 'ALL') AS status,
    COALESCE(holder_region, 'ALL') AS holder_region,
    holder_id,
    GROUPING(status) AS is_total_status,
    ROUND(SUM(holder_risk_score) / NULLIF(SUM(start_date_dayofweek), 0), 3) AS holder_risk_score_to_start_date_dayofweek_ratio
FROM combined_details
GROUP BY GROUPING SETS ((status, holder_region, holder_id), (status), (holder_region), ())
