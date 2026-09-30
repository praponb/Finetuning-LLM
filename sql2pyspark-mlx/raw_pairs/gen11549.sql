WITH valid AS (
    SELECT *, ROUND(quantity * 1.15, 2) AS quantity_scaled
    FROM order_items
    WHERE order_id IN (1, 30, 40)
),
kept_keys AS (
    SELECT order_id
    FROM valid
    EXCEPT ALL
    SELECT order_id
    FROM order_items
    WHERE product_id IN (17, 28)
),
reconciled AS (
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.order_id = k.order_id
),
enriched AS (
    SELECT
        s.order_id,
        s.product_id,
        s.quantity,
        s.unit_price,
        b.amount
    FROM reconciled s
    INNER JOIN orders b
        ON s.order_id = b.id
    WHERE b.customer_id = 14
),
enriched_order_items AS (
    SELECT
        s.*,
        b.price,
        b.launch_date
    FROM enriched s
    LEFT JOIN products b
        ON s.product_id = b.id
)
SELECT
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    product_id,
    PERCENTILE_APPROX(price, 0.5) AS price_p50,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    PERCENTILE_APPROX(unit_price, 0.9) AS unit_price_p90,
    BIT_AND(quantity) AS quantity_bit_and,
    CORR(quantity, unit_price) AS corr_quantity_unit_price,
    GROUPING_ID() AS grouping_level
FROM enriched_order_items
GROUP BY CUBE (DATE_TRUNC('WEEK', launch_date), product_id)
ORDER BY product_id DESC NULLS LAST;
