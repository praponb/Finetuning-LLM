WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label
    FROM customers
    WHERE signup_date >= TO_DATE('2023-07-06')
      AND signup_date <= '2025-10-23'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM base
    QUALIFY RANK() OVER (PARTITION BY segment ORDER BY credit_limit NULLS LAST) = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        is_active_label,
        PERCENT_RANK() OVER w_grp AS age_percent_rank,
        SUM(credit_limit) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
        SUM(credit_limit) OVER (PARTITION BY segment) AS segment_sum_credit_limit,
        AVG(age) OVER w_ordered AS age_avg_5
    FROM ranked
    WINDOW w_grp AS (PARTITION BY city ORDER BY age DESC), w_ordered AS (PARTITION BY city ORDER BY age, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.order_discount
    FROM enriched s
    JOIN LATERAL (
            SELECT c.discount AS order_discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    name,
    email,
    country,
    signup_date,
    is_active,
    credit_limit,
    is_active_label,
    running_credit_limit
FROM latest_match;
