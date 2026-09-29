WITH filtered AS (
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM subscriptions
    WHERE start_date > DATE '2023-09-23'
),
joined AS (
    SELECT
        s.*,
        b.age,
        b.city
    FROM filtered s
    INNER JOIN customers b
        ON s.customer_id = b.id
    WHERE b.country IN ('UK', 'TH', 'US')
),
with_windows AS (
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        age,
        city,
        NTILE(3) OVER (PARTITION BY city ORDER BY age, id) AS age_bucket,
        LEAD(age, 2, 0) OVER (PARTITION BY MONTH(end_date), status ORDER BY monthly_fee, id) AS next_age,
        SUM(monthly_fee) OVER (PARTITION BY YEAR(end_date), plan ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee
    FROM joined
),
metrics AS (
    SELECT
        *,
        SUM(age) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_age,
        MAX(age) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_age,
        AVG(age) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_age
    FROM with_windows
)
SELECT
    id,
    customer_id,
    start_date,
    monthly_fee,
    city,
    running_max_age,
    LAG(running_max_age) OVER w_recent AS prev_running_max_age,
    LEAD(running_max_age) OVER w_recent AS next_running_max_age,
    LAG(age) OVER w_recent AS prev_age,
    ROW_NUMBER() OVER w_recent AS row_seq
FROM metrics
WINDOW w_recent AS (PARTITION BY customer_id ORDER BY age, id)
