WITH filtered AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date
    FROM customers
    WHERE signup_date > DATE '2023-03-14'
      AND segment IS NOT NULL
),
enriched AS (
    SELECT
        id,
        city,
        segment,
        is_active,
        age,
        days_since_signup_date,
        LEAD(age) OVER w_recent AS next_age,
        LAG(days_since_signup_date) OVER w_recent AS prev_days_since_signup_date,
        LEAD(days_since_signup_date) OVER w_recent AS next_days_since_signup_date,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM filtered
    WHERE credit_limit > 14500
    WINDOW w_recent AS (PARTITION BY city, country ORDER BY days_since_signup_date, id)
),
with_latest AS (
    SELECT
        s.*,
        top1.status,
        top1.order_amount
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.status, c.amount AS order_amount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    SELECT
        id,
        city,
        is_active,
        age,
        next_age,
        status,
        LEAD(prev_days_since_signup_date, 1) OVER (PARTITION BY status ORDER BY next_days_since_signup_date, id) AS next_prev_days_since_signup_date,
        FIRST_VALUE(city, TRUE) OVER (PARTITION BY city ORDER BY order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city
    FROM with_latest
)
SELECT
    CASE WHEN age >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    status,
    COUNT_IF(status = 'COMPLETED') AS completed_events,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_count,
    SUM(next_prev_days_since_signup_date) FILTER (WHERE status = 'RETURNED') AS returned_total_next_prev_days_since_signup_date,
    ROUND(SUM(next_prev_days_since_signup_date) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS next_prev_days_since_signup_date_per_new_york,
    COUNT_IF(status = 'COMPLETED') AS completed_events_2,
    GROUPING_ID() AS grouping_id
FROM metrics
GROUP BY ROLLUP (CASE WHEN age >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END, status)
ORDER BY status DESC NULLS FIRST
