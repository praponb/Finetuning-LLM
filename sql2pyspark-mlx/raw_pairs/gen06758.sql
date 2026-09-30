WITH valid AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ROUND(quantity * 1.1, 2) AS quantity_adjusted
    FROM order_items
    WHERE product_id IS NOT NULL
),
joined AS (
    SELECT a.*, p.discount
    FROM valid a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity_adjusted, id) AS latest_rank
        FROM joined
    ) x
    WHERE latest_rank = 1
),
joined_order_items AS (
    SELECT
        e.*,
        d.product_name,
        d.launch_date
    FROM ranked e
    LEFT JOIN products d
        ON e.product_id = d.id
)
SELECT
    id,
    product_id,
    quantity,
    unit_price,
    quantity_adjusted,
    launch_date
FROM joined_order_items
