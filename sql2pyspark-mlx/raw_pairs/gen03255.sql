WITH cleaned AS (
    SELECT *, SUBSTRING(plan, 1, 3) plan_prefix
    FROM subscriptions
    WHERE monthly_fee < 200
)
SELECT *
FROM (
    SELECT
        customer_id,
        plan,
        monthly_fee
    FROM cleaned
) src
PIVOT (
    SUM(monthly_fee) AS total
    FOR plan IN ('enterprise' AS enterprise, 'standard' AS standard)
);
