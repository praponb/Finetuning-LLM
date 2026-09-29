SELECT
    plan,
    status,
    end_date,
    CASE WHEN status IN ('EXPIRED', 'ACTIVE') THEN 'Core' ELSE 'Other' END AS status_group,
    COALESCE(monthly_fee, 0) AS monthly_fee_filled
FROM subscriptions
WHERE customer_id = 23 AND (MONTH(end_date) IN (2, 3, 5) OR monthly_fee BETWEEN 110 AND 210)
