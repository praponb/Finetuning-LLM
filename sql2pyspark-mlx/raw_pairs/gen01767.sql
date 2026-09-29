WITH filtered AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        COALESCE(monthly_fee, 0) AS monthly_fee_filled,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM subscriptions
    WHERE end_date > DATE '2023-05-25'
      AND customer_id IS NOT NULL
),
with_dims AS (
    SELECT
        src.id,
        src.customer_id,
        src.plan,
        src.start_date,
        src.monthly_fee_filled,
        src.days_start_date_to_end_date,
        d.age,
        d.signup_date,
        d.segment
    FROM filtered src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
joined AS (
    SELECT
        a.*,
        p.email,
        p.city
    FROM with_dims a
    INNER JOIN customers p
        ON a.customer_id = p.id
),
joined_subscriptions AS (
    SELECT
        s.*,
        d.country,
        d.signup_date AS customer_signup_date,
        d.age AS customer_age
    FROM joined s
    LEFT JOIN customers d
        ON s.customer_id <=> d.id
    WHERE d.country = 'US'
),
metrics AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee_filled,
        age,
        email,
        FIRST_VALUE(days_start_date_to_end_date, TRUE) OVER w_recent AS first_known_days_start_date_to_end_date,
        MAX(days_start_date_to_end_date) OVER (PARTITION BY country) AS country_max_days_start_date_to_end_date,
        AVG(days_start_date_to_end_date) OVER (PARTITION BY MONTH(customer_signup_date), plan ORDER BY signup_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS days_start_date_to_end_date_avg_3,
        AVG(monthly_fee_filled) OVER w_ordered AS monthly_fee_filled_avg_6,
        SUM(customer_age) OVER w_hist AS running_customer_age
    FROM joined_subscriptions
    WINDOW w_recent AS (PARTITION BY customer_id, country ORDER BY customer_signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY country ORDER BY age ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY country ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT
        id,
        customer_id,
        monthly_fee_filled,
        age,
        email,
        country_max_days_start_date_to_end_date,
        running_customer_age,
        SUM(days_start_date_to_end_date_avg_3) OVER (PARTITION BY YEAR(start_date), customer_id ORDER BY running_customer_age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_start_date_to_end_date_avg_3,
        ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY first_known_days_start_date_to_end_date ASC, id ASC) AS event_seq
    FROM metrics
)
SELECT
    customer_id,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_windows
GROUP BY 1
ORDER BY customer_id ASC NULLS FIRST;
