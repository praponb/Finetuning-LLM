WITH cleaned AS (
    SELECT
        *,
        CONCAT_WS('-', plan, status) AS plan_status_key,
        LENGTH(plan) AS plan_length
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND monthly_fee IS NULL
)
SELECT
    p.city,
    p.country,
    COUNT(*) AS cnt,
    ROUND(AVG(src.plan_length), 2) AS avg_plan_length,
    MIN(src.monthly_fee) AS min_monthly_fee,
    GROUPING_ID() AS gid
FROM cleaned src
INNER JOIN customers p ON src.customer_id = p.id
GROUP BY CUBE (p.city, p.country);
