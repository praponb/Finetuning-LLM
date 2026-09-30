WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        shipping_fee + amount AS amount_with_shipping,
        CAST(amount AS INT) AS amount_int
    FROM orders
    WHERE channel IS DISTINCT FROM 'store'
      AND order_date >= DATE '2023-10-05'
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.*,
        d.city,
        d.store_name,
        d.opened_date
    FROM valid s
    LEFT JOIN stores d
        ON s.store_id <=> d.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(amount_int) OVER (PARTITION BY store_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_int,
        AVG(amount_with_shipping) OVER w_part AS amount_with_shipping_avg_2
    FROM enriched
    WHERE order_date < '2024-03-01'
    WINDOW w_part AS (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
scored_orders AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status,
        discount,
        shipping_fee,
        amount_with_shipping,
        amount_int,
        opened_date,
        amount_with_shipping_avg_2,
        SUM(amount_int) OVER (PARTITION BY status ORDER BY amount_with_shipping_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount_int,
        MAX(amount_int) OVER (PARTITION BY status ORDER BY amount_with_shipping_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount_int,
        AVG(amount_int) OVER (PARTITION BY status ORDER BY amount_with_shipping_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount_int
    FROM scored
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        status,
        discount,
        shipping_fee,
        opened_date,
        amount_with_shipping_avg_2,
        running_sum_amount_int,
        running_max_amount_int
    FROM scored_orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipping_fee DESC, id) = 1
)
SELECT
    id,
    discount,
    shipping_fee,
    opened_date,
    amount_with_shipping_avg_2,
    running_sum_amount_int,
    running_max_amount_int
FROM ranked;
