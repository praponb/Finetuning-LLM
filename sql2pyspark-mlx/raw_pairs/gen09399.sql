WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        shipping_fee,
        DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date
    FROM orders
    WHERE store_id IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.order_id = 40
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at >= s.order_date
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, customer_id ORDER BY shipping_fee DESC NULLS LAST, id) AS latest_rank
    FROM kept
    QUALIFY latest_rank = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        order_date,
        amount,
        shipping_fee,
        days_since_order_date,
        latest_rank,
        AVG(days_since_order_date) OVER w_running AS days_since_order_date_avg_2_prior,
        FIRST_VALUE(discount, TRUE) OVER (ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount,
        AVG(amount) OVER (PARTITION BY DATE_TRUNC('month', order_date) ORDER BY shipping_fee, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior,
        PERCENT_RANK() OVER w_ordered AS discount_percent_rank
    FROM ranked
    WINDOW w_running AS (ORDER BY days_since_order_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_ordered AS (PARTITION BY customer_id ORDER BY discount DESC NULLS FIRST)
)
-- Join a dimension and aggregate
SELECT
    s.city,
    COUNT(*) AS rows_total,
    AVG(a.days_since_order_date_avg_2_prior) AS avg_days_since_order_date_avg_2_prior
FROM scored a
LEFT JOIN stores s ON a.store_id = s.id
GROUP BY s.city
ORDER BY rows_total NULLS LAST
