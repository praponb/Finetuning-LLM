WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), order_date) days_since_order_date
    FROM orders
    WHERE order_date >= DATE '2023-07-05'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        store_id,
        discount,
        days_since_order_date,
        MAX(shipping_fee) OVER (PARTITION BY store_id) store_id_max_shipping_fee,
        AVG(days_since_order_date) OVER (PARTITION BY MONTH(order_date), store_id ORDER BY shipping_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) days_since_order_date_avg_3,
        AVG(amount) OVER (PARTITION BY customer_id ORDER BY order_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) amount_avg_2,
        AVG(shipping_fee) OVER (PARTITION BY channel) channel_avg_shipping_fee
    FROM valid
    WHERE store_id IS NOT NULL
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.days_since_order_date_avg_3,
        p.sqft,
        p.opened_date,
        p.store_name
    FROM with_windows s
    LEFT JOIN stores p
        ON s.store_id <=> p.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT store_id, customer_id
    FROM with_dims
    EXCEPT
    SELECT store_id, customer_id
    FROM orders
    WHERE order_date <= '2023-05-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.store_id = k.store_id AND s.customer_id = k.customer_id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        store_id,
        DATE_TRUNC('MONTH', opened_date) AS activity_month,
        MIN(sqft) AS min_sqft,
        ROUND(AVG(sqft), 2) AS avg_sqft,
        COUNT(DISTINCT store_id) AS distinct_store_ids,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(days_since_order_date_avg_3, 0.9) AS days_since_order_date_avg_3_p90
    FROM verified
    GROUP BY 1, 2
)
SELECT
    s.store_id,
    s.activity_month,
    s.min_sqft,
    s.distinct_store_ids,
    s.days_since_order_date_avg_3_p90,
    (SELECT SUM(x.quantity) FROM inventory x WHERE x.store_id = s.store_id) AS sum_inventory_quantity
FROM stats s
ORDER BY activity_month NULLS FIRST;
