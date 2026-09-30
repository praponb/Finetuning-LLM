WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        FLOOR(quantity) quantity_whole,
        CASE WHEN unit_price >= 420 THEN 'High' WHEN unit_price >= 70 THEN 'Medium' ELSE 'Low' END unit_price_band
    FROM order_items
    WHERE product_id IS NOT NULL
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.quantity,
        e.unit_price,
        e.unit_price_band,
        b.shipping_fee
    FROM filtered e
    INNER JOIN orders b
        ON e.order_id = b.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        unit_price,
        shipping_fee,
        RANK() OVER (PARTITION BY order_id ORDER BY shipping_fee DESC) shipping_fee_rank,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY unit_price NULLS FIRST) unit_price_percent_rank,
        CORR(unit_price, quantity) OVER (PARTITION BY order_id) corr_unit_price_quantity
    FROM joined
    WHERE unit_price IS NOT NULL
),
scored_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY order_id ORDER BY shipping_fee, id) shipping_fee_bucket,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipping_fee_rank DESC, id) event_seq,
        AVG(corr_unit_price_quantity) OVER (PARTITION BY order_id ORDER BY shipping_fee_rank, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) corr_unit_price_quantity_avg_2_prior
    FROM scored
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, unit_price * shipping_fee_bucket unit_price_times_shipping_fee_bucket
    FROM scored_order_items
    WHERE corr_unit_price_quantity < 0.6
)
SELECT
    s.id,
    s.order_id,
    s.unit_price,
    s.shipping_fee,
    s.shipping_fee_rank,
    s.unit_price_percent_rank,
    s.corr_unit_price_quantity,
    s.shipping_fee_bucket,
    s.event_seq,
    (SELECT AVG(o2.unit_price) FROM order_items o2 WHERE o2.order_id = s.order_id) AS avg_order_item_unit_price
FROM cleaned s
