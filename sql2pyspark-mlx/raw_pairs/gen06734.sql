WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(monthly_fee, TRUE) OVER w_ordered AS first_known_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS row_seq
    FROM subscriptions
    WHERE end_date BETWEEN '2023-09-01' AND '2023-10-01'
    WINDOW w_ordered AS (PARTITION BY customer_id, plan ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    customer_id,
    plan,
    start_date,
    monthly_fee,
    status,
    first_known_monthly_fee,
    row_seq
FROM metrics
