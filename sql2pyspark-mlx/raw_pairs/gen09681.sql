SELECT
    id,
    status,
    customer_id,
    monthly_fee,
    DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
FROM subscriptions
WHERE monthly_fee BETWEEN 170 AND 180
