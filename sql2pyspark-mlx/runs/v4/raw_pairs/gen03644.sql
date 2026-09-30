WITH base AS (
    SELECT
        *,
        LENGTH(plan) plan_length,
        TRIM(plan) clean_plan
    FROM subscriptions
    WHERE end_date BETWEEN '2023-03-02' AND '2025-10-21'
)
SELECT *
FROM (
    SELECT
        customer_id,
        status,
        plan,
        plan_length
    FROM base
) src
PIVOT (
    AVG(plan_length) AS amount,
    COUNT(1) AS n
    FOR plan IN ('premium', 'enterprise')
)
