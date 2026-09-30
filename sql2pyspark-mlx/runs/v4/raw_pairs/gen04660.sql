WITH base AS (
    SELECT
        *,
        SUBSTRING(status, 1, 4) status_prefix,
        LOWER(status) status_lower
    FROM subscriptions
    WHERE end_date >= DATE '2023-09-04'
)
SELECT *
FROM (
    SELECT
        status,
        plan,
        monthly_fee
    FROM base
) src
PIVOT (
    MAX(monthly_fee) AS amount,
    COUNT(1) AS n
    FOR plan IN ('basic' AS basic, 'standard' AS standard)
);
