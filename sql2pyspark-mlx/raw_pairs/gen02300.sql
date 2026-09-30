WITH joined AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.product_id,
        a.quantity,
        a.unit_price,
        d.order_date,
        d.amount,
        d.discount
    FROM order_items a
    LEFT JOIN orders d
        ON a.order_id = d.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        order_date,
        RANK() OVER w_ordered AS unit_price_rank,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
    FROM joined
    WINDOW w_ordered AS (PARTITION BY MONTH(order_date), product_id ORDER BY unit_price DESC NULLS FIRST)
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.unit_price,
        src.order_date,
        src.unit_price_rank,
        b.order_date AS order_order_date,
        b.discount
    FROM scored src
    INNER JOIN orders b
        ON src.order_id = b.id
),
joined_order_items AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.order_date AS order_order_date_2,
        b.status
    FROM enriched a
    LEFT JOIN orders b
        ON a.order_id = b.id
    WHERE b.discount > 0.2
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY unit_price NULLS FIRST, id) AS rank_in_group
    FROM joined_order_items
    QUALIFY rank_in_group = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('QUARTER', order_order_date) AS quarter_start,
        order_id,
        COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_count,
        COUNT_IF(status = 'SHIPPED') AS shipped_events,
        GROUPING_ID() AS gid
    FROM deduped
    GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', order_order_date), order_id), (DATE_TRUNC('QUARTER', order_order_date)), ())
)
SELECT
    s.quarter_start,
    s.order_id,
    s.pending_count,
    s.shipped_events,
    (SELECT MAX(x.amount) FROM payments x WHERE x.order_id = s.order_id) AS max_payment_amount
FROM stats s
