WITH joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.order_date,
        p.shipping_fee
    FROM order_items e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        order_date,
        MIN(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_unit_price,
        AVG(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_unit_price,
        COUNT(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_unit_price,
        SUM(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_unit_price,
        CORR(unit_price, quantity) OVER (PARTITION BY order_id) AS corr_unit_price_quantity
    FROM joined
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        order_date,
        last3_min_unit_price,
        last3_avg_unit_price,
        last3_count_unit_price,
        last3_sum_unit_price,
        corr_unit_price_quantity,
        ROUND(last3_min_unit_price, 1) AS last3_min_unit_price_rounded
    FROM enriched
    WHERE last3_sum_unit_price > 1700
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY last3_avg_unit_price NULLS FIRST, id) AS rn
    FROM base
    QUALIFY rn = 1
)
SELECT
    s.id,
    s.order_id,
    s.quantity,
    s.unit_price,
    s.order_date,
    s.last3_min_unit_price,
    s.last3_count_unit_price,
    s.last3_sum_unit_price,
    s.corr_unit_price_quantity,
    (SELECT MIN(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS min_shipment_cost
FROM latest s
