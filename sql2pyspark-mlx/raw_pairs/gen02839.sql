WITH base AS (
    SELECT *, FLOOR(unit_price) AS unit_price_whole
    FROM order_items
    WHERE product_id IS NULL
),
enriched AS (
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.quantity,
        p.product_name,
        p.brand,
        p.cost
    FROM base src
    LEFT JOIN products p
        ON src.product_id = p.id
    WHERE p.category = 'Fashion'
),
with_windows AS (
    SELECT
        id,
        order_id,
        quantity,
        NTILE(4) OVER (ORDER BY quantity DESC, id ASC) AS quantity_bucket,
        LAST_VALUE(cost, TRUE) OVER w_running AS last_known_cost
    FROM enriched
    WINDOW w_running AS (PARTITION BY brand ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_order_items AS (
    SELECT
        src.*,
        o.discount,
        o.status
    FROM with_windows src
    LEFT JOIN orders o
        ON src.order_id = o.id
)
SELECT
    CASE WHEN quantity >= 9 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
    status,
    APPROX_PERCENTILE(last_known_cost, ARRAY(0.25, 0.5, 0.75)) AS last_known_cost_quartiles,
    MAX(discount) AS max_discount,
    BIT_OR(quantity) AS quantity_bit_or,
    GROUPING(CASE WHEN quantity >= 9 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
FROM enriched_order_items
GROUP BY GROUPING SETS ((CASE WHEN quantity >= 9 THEN 'HIGH' ELSE 'LOW' END, status), (CASE WHEN quantity >= 9 THEN 'HIGH' ELSE 'LOW' END), ())
ORDER BY quantity_tier DESC NULLS LAST;
