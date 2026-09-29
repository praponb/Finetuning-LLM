WITH base AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        CAST(unit_price AS INT) AS unit_price_int
    FROM order_items
    WHERE product_id IS NOT NULL
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price, id) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
base_order_items AS (
    SELECT
        id,
        product_id,
        unit_price_int,
        rn,
        COALESCE(quantity, 0) AS quantity_filled
    FROM ranked
    WHERE quantity BETWEEN 7 AND 8
),
enriched AS (
    SELECT
        *,
        MIN(unit_price_int) OVER w_running AS running_min_unit_price_int,
        AVG(unit_price_int) OVER w_running AS running_avg_unit_price_int,
        SUM(unit_price_int) OVER w_running AS running_sum_unit_price_int,
        DENSE_RANK() OVER w_grp AS unit_price_int_dense_rank
    FROM base_order_items
    WHERE unit_price_int BETWEEN 62 AND 68
    WINDOW w_running AS (PARTITION BY product_id ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY product_id ORDER BY unit_price_int DESC)
)
SELECT
    id,
    product_id,
    unit_price_int,
    running_sum_unit_price_int,
    unit_price_int_dense_rank
FROM enriched
ORDER BY product_id DESC;
