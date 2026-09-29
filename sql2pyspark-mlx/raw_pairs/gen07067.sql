WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        ADD_MONTHS(order_date, 12) AS order_date_plus_months
    FROM orders
    WHERE order_date > TO_DATE('2023-07-23')
      AND channel IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        order_date_plus_months
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, store_id ORDER BY amount DESC, id) = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(DATEDIFF(order_date_plus_months, order_date) / 30, 1) AS months_order_date_to_order_date_plus_months
    FROM latest
    WHERE customer_id IS NOT NULL
      AND order_date_plus_months BETWEEN '2023-07-01' AND '2024-07-01'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(amount) OVER (PARTITION BY store_id ORDER BY order_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
        COUNT(amount) OVER (PARTITION BY store_id ORDER BY order_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount,
        MIN(amount) OVER (PARTITION BY store_id ORDER BY order_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount,
        SUM(amount) OVER (PARTITION BY store_id ORDER BY order_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount
    FROM base
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        order_date_plus_months,
        months_order_date_to_order_date_plus_months,
        running_max_amount,
        running_min_amount,
        AVG(running_count_amount) OVER (PARTITION BY status ORDER BY running_sum_amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS running_count_amount_avg_2_prior,
        LAG(running_sum_amount, 2) OVER (PARTITION BY store_id ORDER BY running_count_amount, id) AS prev_running_sum_amount,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY amount) AS amount_dense_rank,
        LEAD(running_sum_amount, 2) OVER (PARTITION BY status ORDER BY months_order_date_to_order_date_plus_months, id) AS next_running_sum_amount,
        RANK() OVER (PARTITION BY status ORDER BY amount DESC NULLS LAST) AS amount_rank
    FROM scored
    WHERE status = 'CANCELLED'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', order_date_plus_months) AS week_start,
    CASE WHEN months_order_date_to_order_date_plus_months >= 61 THEN 'LARGE' ELSE 'SMALL' END AS months_order_date_to_order_date_plus_months_tier,
    CASE WHEN amount_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS amount_rank_tier,
    ROUND(SUM(running_max_amount) / NULLIF(COUNT(CASE WHEN status = 'RETURNED' THEN 1 END), 0), 2) AS running_max_amount_per_returned,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count
FROM with_windows
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', order_date_plus_months), CASE WHEN months_order_date_to_order_date_plus_months >= 61 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN amount_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('WEEK', order_date_plus_months), CASE WHEN months_order_date_to_order_date_plus_months >= 61 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('WEEK', order_date_plus_months)), ());
