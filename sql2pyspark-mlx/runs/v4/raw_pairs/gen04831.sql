WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        SUBSTRING(status, 1, 4) AS status_prefix,
        LAST_DAY(start_date) AS start_date_month_end
    FROM subscriptions
    WHERE start_date > TO_DATE('2023-10-10')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        status,
        plan,
        monthly_fee
    FROM base
) src
PIVOT (
    SUM(monthly_fee) AS total
    FOR plan IN ('enterprise', 'standard')
);
