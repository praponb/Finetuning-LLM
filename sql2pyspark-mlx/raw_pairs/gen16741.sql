SELECT
    id,
    plan,
    start_date,
    end_date,
    LEAD(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date, id) next_monthly_fee,
    LAG(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date, id) prev_monthly_fee,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date, id) event_seq
FROM subscriptions
WHERE monthly_fee BETWEEN 90 AND 120;
