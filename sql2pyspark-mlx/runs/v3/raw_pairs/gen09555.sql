SELECT
    id,
    plan,
    end_date,
    status
FROM (
    SELECT
        id,
        plan,
        end_date,
        status,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) rnk
    FROM subscriptions
) ranked
WHERE rnk <= 3;
