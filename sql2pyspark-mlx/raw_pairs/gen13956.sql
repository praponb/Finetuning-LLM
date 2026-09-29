WITH with_dims AS (
    -- Attach products attributes
    SELECT a.*, p.cost
    FROM order_items a
    INNER JOIN products p
        ON a.product_id = p.id
    WHERE p.category IN ('Fashion', 'Electronics', 'Home')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        cost
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity DESC, id) = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.quantity,
        src.cost,
        o.order_date,
        o.amount
    FROM deduped src
    INNER JOIN orders o
        ON src.order_id = o.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        cost,
        amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity, id) AS row_seq,
        LAST_VALUE(quantity, TRUE) OVER w_recent AS last_known_quantity
    FROM joined
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        cost,
        amount,
        row_seq,
        last_known_quantity,
        last_known_quantity + quantity AS last_known_quantity_plus_quantity,
        amount + cost AS amount_plus_cost
    FROM enriched
    WHERE quantity < 6
)
SELECT
    id,
    cost,
    amount,
    last_known_quantity,
    last_known_quantity_plus_quantity
FROM filtered;
