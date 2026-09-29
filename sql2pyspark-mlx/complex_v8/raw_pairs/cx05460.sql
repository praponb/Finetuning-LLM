WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        ADD_MONTHS(start_date, 3) AS start_date_plus_3m,
        CASE WHEN premium >= 3521.8 THEN 'HIGH' WHEN premium >= 1132.1 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier,
        DAYOFWEEK(end_date) AS end_date_dayofweek
    FROM policies
    WHERE product = 'HEALTH'
      AND holder_id IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        status,
        start_date_plus_3m,
        premium_tier,
        end_date_dayofweek,
        SUM(premium) OVER (PARTITION BY YEAR(start_date_plus_3m) ORDER BY start_date_plus_3m NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROW_NUMBER() OVER (PARTITION BY YEAR(start_date_plus_3m) ORDER BY start_date_plus_3m NULLS LAST, id) AS seq_no
    FROM prepared_policys
),
with_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.start_date_plus_3m,
        s.end_date_dayofweek,
        s.running_premium,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region,
        p.holder_name
    FROM ranked s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.region = 'East'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        status,
        end_date_dayofweek,
        running_premium,
        holder_region,
        holder_name,
        LEAD(running_premium) OVER (PARTITION BY holder_region, status ORDER BY start_date_plus_3m ASC, id) AS next_running_premium,
        FIRST_VALUE(holder_risk_score) IGNORE NULLS OVER (PARTITION BY holder_region, status ORDER BY start_date_plus_3m ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_holder_risk_score
    FROM with_details_holder
)
SELECT id, measure, value
FROM (
    SELECT id, running_premium, first_known_holder_risk_score
    FROM metrics
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (running_premium, first_known_holder_risk_score)
) unp
