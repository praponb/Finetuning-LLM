SELECT
    id,
    plan,
    monthly_fee,
    end_date
FROM (
    SELECT
        id,
        plan,
        monthly_fee,
        end_date,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee, id) rnk
    FROM subscriptions
) r
WHERE rnk <= 1
