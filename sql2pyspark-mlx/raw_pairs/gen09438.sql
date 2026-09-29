WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(monthly_fee AS INT) monthly_fee_int
    FROM subscriptions
    WHERE end_date > TO_DATE('2023-08-16')
)
-- Join a dimension and aggregate
SELECT
    p.city,
    p.country,
    src.plan,
    COUNT(*) AS rows_total,
    ROUND(AVG(src.monthly_fee), 2) AS avg_monthly_fee,
    ROUND(AVG(src.monthly_fee_int), 2) AS avg_monthly_fee_int,
    GROUPING_ID() AS gid
FROM base src
INNER JOIN customers p ON src.customer_id = p.id
GROUP BY GROUPING SETS ((p.city, p.country, src.plan), (p.city), ());
