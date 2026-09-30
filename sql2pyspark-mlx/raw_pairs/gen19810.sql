WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(end_date, start_date) / 7 weeks_start_date_to_end_date
    FROM subscriptions
    WHERE monthly_fee <= 150
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    customer_id,
    kpi,
    val
FROM (
    SELECT
        id,
        customer_id,
        monthly_fee,
        weeks_start_date_to_end_date
    FROM filtered
) src
UNPIVOT INCLUDE NULLS (
    val FOR kpi IN (monthly_fee, weeks_start_date_to_end_date)
)
