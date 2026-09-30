WITH scored AS (
    SELECT
        *,
        SUM(quantity) OVER w_grp cumulative_quantity,
        AVG(unit_price) OVER (PARTITION BY order_id ORDER BY quantity ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) unit_price_avg_5,
        CORR(quantity, unit_price) OVER w_running corr_quantity_unit_price
    FROM order_items
    WHERE unit_price <= 130
    WINDOW w_grp AS (ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY order_id, product_id)
),
with_windows AS (
    SELECT
        *,
        NTILE(10) OVER w_recent unit_price_avg_5_bucket,
        SUM(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_unit_price,
        AVG(unit_price_avg_5) OVER w_ordered unit_price_avg_5_avg_2_prior
    FROM scored
    WINDOW w_recent AS (ORDER BY unit_price_avg_5 ASC, id ASC), w_ordered AS (PARTITION BY product_id ORDER BY unit_price_avg_5 ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
joined AS (
    SELECT a.*, p.product_name
    FROM with_windows a
    JOIN products p
        ON a.product_id = p.id
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price_avg_5_avg_2_prior DESC, id ASC) AS rank_in_group
        FROM joined
    ) x
    WHERE rank_in_group = 1
),
matched_keys AS (
    SELECT order_id
    FROM ranked
    EXCEPT ALL
    SELECT order_id
    FROM order_items
    WHERE product_id IS NOT NULL
),
kept_rows AS (
    SELECT s.*
    FROM ranked s
    JOIN matched_keys k ON s.order_id = k.order_id
)
SELECT
    s.id,
    s.order_id,
    s.quantity,
    s.unit_price,
    s.cumulative_quantity,
    s.unit_price_avg_5,
    s.corr_quantity_unit_price,
    s.running_unit_price,
    s.unit_price_avg_5_avg_2_prior,
    (SELECT MAX(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS max_shipment_cost
FROM kept_rows s;
