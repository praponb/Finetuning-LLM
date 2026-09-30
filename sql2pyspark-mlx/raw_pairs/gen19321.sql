WITH filtered AS (
    SELECT *, COALESCE(revenue, 0) AS revenue_filled
    FROM store_sales
    WHERE product_id IN (1, 4, 9)
),
joined AS (
    SELECT
        e.id,
        e.product_id,
        e.sale_date,
        e.units,
        d.launch_date
    FROM filtered e
    LEFT JOIN products d
        ON e.product_id = d.id
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date, id) AS rank_in_group
    FROM joined
    QUALIFY rank_in_group = 1
),
latest AS (
    SELECT
        id,
        product_id,
        launch_date
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date NULLS FIRST, id) = 1
),
metrics AS (
    SELECT
        id,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY MONTH(launch_date), product_id ORDER BY launch_date, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY launch_date, id) AS seq_no
    FROM latest
)
SELECT
    s.id,
    s.product_id,
    s.row_seq,
    s.seq_no,
    (SELECT AVG(t2.quantity) FROM order_items t2 WHERE t2.product_id = s.product_id) AS avg_order_item_quantity
FROM metrics s
