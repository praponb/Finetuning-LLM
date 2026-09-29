WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_date DESC, id) AS latest_rank
    FROM orders
    QUALIFY latest_rank = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        latest_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount NULLS FIRST, id) AS rn
        FROM latest
    ) t
    WHERE rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(discount) OVER w_part AS last7_min_discount,
        SUM(discount) OVER w_part AS last7_sum_discount,
        COUNT(discount) OVER w_part AS last7_count_discount,
        AVG(discount) OVER w_part AS last7_avg_discount,
        RANK() OVER (PARTITION BY customer_id ORDER BY discount DESC NULLS LAST) AS discount_rank
    FROM ranked
    WINDOW w_part AS (PARTITION BY status ORDER BY latest_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
with_windows_orders AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        latest_rank,
        last7_min_discount,
        last7_count_discount,
        discount_rank,
        RANK() OVER (ORDER BY discount NULLS LAST) AS discount_rank_2,
        MIN(last7_sum_discount) OVER w_hist AS channel_min_last7_sum_discount,
        MAX(last7_min_discount) OVER (PARTITION BY customer_id) AS customer_id_max_last7_min_discount,
        RANK() OVER (PARTITION BY channel ORDER BY last7_min_discount DESC NULLS LAST) AS last7_min_discount_rank,
        PERCENT_RANK() OVER (ORDER BY amount) AS amount_percent_rank
    FROM with_windows
    WHERE channel = 'partner'
    WINDOW w_hist AS (PARTITION BY channel)
),
ranked_orders AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows_orders
    QUALIFY RANK() OVER (PARTITION BY channel ORDER BY last7_min_discount DESC) = 1
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.name,
        p.signup_date
    FROM ranked_orders s
    LEFT JOIN customers p
        ON s.customer_id <=> p.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.channel,
        e.amount,
        e.discount,
        e.channel_min_last7_sum_discount,
        e.signup_date,
        c.age
    FROM enriched e
    LEFT JOIN customers c
        ON e.customer_id = c.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        channel,
        discount,
        amount
    FROM with_dims
) src
UNPIVOT (
    value FOR kpi IN (discount AS DISCOUNT, amount AS AMOUNT_VAL)
);
