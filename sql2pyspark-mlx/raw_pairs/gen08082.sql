WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.order_date,
        o.channel,
        o.amount
    FROM order_items src
    INNER JOIN orders o
        ON src.order_id = o.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        amount,
        FLOOR(unit_price) AS unit_price_whole,
        unit_price - amount AS unit_price_minus_amount
    FROM with_dims
    WHERE order_date > '2025-05-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_quantity,
        MIN(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_quantity,
        AVG(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_quantity,
        SUM(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_quantity
    FROM base
    WHERE order_id IN (7, 9, 25, 34)
),
joined AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.quantity,
        a.unit_price,
        a.amount,
        a.unit_price_minus_amount,
        a.last3_count_quantity,
        a.last3_sum_quantity,
        d.stock_qty,
        d.cost
    FROM with_windows a
    LEFT JOIN products d
        ON a.product_id = d.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY last3_sum_quantity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY unit_price_minus_amount) AS unit_price_minus_amount_cume_dist,
        LEAD(unit_price_minus_amount, 1) OVER (PARTITION BY product_id ORDER BY amount, id) AS next_unit_price_minus_amount
    FROM joined
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN cost >= 670 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
        CASE WHEN last3_count_quantity >= 16 THEN 'HIGH' ELSE 'LOW' END AS last3_count_quantity_tier,
        APPROX_PERCENTILE(stock_qty, 0.95) AS stock_qty_p95,
        COUNT(*) AS num_rows,
        BIT_AND(quantity) AS quantity_bit_and,
        RANK() OVER (PARTITION BY CASE WHEN cost >= 670 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(*) DESC) AS num_rows_rank
    FROM enriched
    GROUP BY CASE WHEN cost >= 670 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last3_count_quantity >= 16 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    cost_tier,
    last3_count_quantity_tier,
    quantity_bit_and,
    num_rows_rank
FROM grouped
ORDER BY last3_count_quantity_tier DESC, quantity_bit_and;
