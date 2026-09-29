SELECT
    CASE WHEN discount >= 0.41 THEN 'high' WHEN discount >= 0.07 THEN 'mid' ELSE 'low' END AS discount_bucket,
    CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort,
    COUNT(discount) AS discount_count,
    SUM(amount) AS total_amount,
    SUM(CASE WHEN channel = 'mobile' THEN discount ELSE 0 END) AS mobile_discount
FROM orders
GROUP BY CASE WHEN discount >= 0.41 THEN 'high' WHEN discount >= 0.07 THEN 'mid' ELSE 'low' END, CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END
