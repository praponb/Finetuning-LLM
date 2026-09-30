WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        CORR(unit_price, quantity) OVER (PARTITION BY order_id, product_id) AS corr_unit_price_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        ROW_NUMBER() OVER (ORDER BY quantity ASC, id ASC) AS seq_no,
        LAST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity
    FROM order_items
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY seq_no ASC NULLS FIRST) AS row_num
        FROM metrics
    ) x
    WHERE row_num = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        d.amount,
        d.status
    FROM latest src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        d.launch_date,
        d.stock_qty
    FROM joined a
    INNER JOIN products d
        ON a.product_id = d.id
),
joined_order_items AS (
    -- Attach orders attributes
    SELECT
        a.*,
        p.status AS order_status,
        p.discount,
        p.order_date
    FROM with_dims a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.*,
        b.category,
        b.launch_date AS product_launch_date,
        b.price
    FROM joined_order_items e
    LEFT JOIN products b
        ON e.product_id = b.id
)
-- Running and moving aggregates
SELECT
    *,
    FIRST_VALUE(running_quantity) IGNORE NULLS OVER (PARTITION BY order_status ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_quantity,
    SUM(running_quantity) OVER (PARTITION BY status) AS status_sum_running_quantity,
    SUM(corr_unit_price_quantity) OVER (PARTITION BY category) AS category_sum_corr_unit_price_quantity
FROM enriched
