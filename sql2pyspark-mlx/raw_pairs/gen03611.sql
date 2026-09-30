WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(plan) AS plan_length,
        ROUND(DATEDIFF(end_date, start_date) / 30, 1) AS months_start_date_to_end_date
    FROM subscriptions
    WHERE end_date >= DATE '2023-06-06'
      AND customer_id IN (3, 8)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(plan_length) OVER w_part AS running_avg_plan_length,
        SUM(plan_length) OVER w_part AS running_sum_plan_length,
        MAX(plan_length) OVER w_part AS running_max_plan_length,
        LEAD(months_start_date_to_end_date, 1, 0) OVER (ORDER BY months_start_date_to_end_date ASC, id ASC) AS next_months_start_date_to_end_date
    FROM filtered
    WINDOW w_part AS (PARTITION BY status, plan ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY start_date DESC NULLS LAST, id ASC) AS rn
    FROM metrics
    QUALIFY rn = 1
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.country,
        c.credit_limit
    FROM deduped e
    INNER JOIN customers c
        ON e.customer_id <=> c.id
    WHERE c.credit_limit >= 13900
)
SELECT
    s.id,
    s.customer_id,
    s.start_date,
    s.monthly_fee,
    s.status,
    s.months_start_date_to_end_date,
    s.running_avg_plan_length,
    s.running_sum_plan_length,
    s.next_months_start_date_to_end_date,
    (SELECT SUM(o2.shipping_fee) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS sum_order_shipping_fee
FROM with_dims s
ORDER BY months_start_date_to_end_date DESC;
