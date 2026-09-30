WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        category,
        price,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        DAYOFWEEK(launch_date) AS launch_date_dayofweek,
        ARRAY_JOIN(tags, '|') AS tags_text
    FROM products
    WHERE stock_qty >= 173
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        tags,
        launch_date_dayofweek,
        LEAD(launch_date_dayofweek) OVER w_running AS next_launch_date_dayofweek,
        LAG(launch_date_dayofweek) OVER w_running AS prev_launch_date_dayofweek,
        LAG(stock_qty) OVER w_running AS prev_stock_qty,
        ROW_NUMBER() OVER w_running AS seq_no,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM cleaned
    WINDOW w_running AS (PARTITION BY supplier_id ORDER BY price ASC, id ASC), w_part AS (PARTITION BY category ORDER BY price DESC, id ASC)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.quantity <= 8
        )
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.category,
        s.next_launch_date_dayofweek,
        s.prev_stock_qty,
        s.seq_no,
        s.event_seq,
        latest.order_item_quantity
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, order_item_quantity - seq_no AS order_item_quantity_minus_seq_no
    FROM latest_match
    WHERE category = 'Home'
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.category,
        s.next_launch_date_dayofweek,
        s.event_seq,
        latest.quantity
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN quantity >= 198 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
        COUNT(*) FILTER (WHERE category = 'Sports') AS sports_rows,
        ROUND(AVG(quantity), 2) AS avg_quantity,
        COUNT(*) FILTER (WHERE category = 'Sports') AS sports_rows_2
    FROM with_latest
    GROUP BY CASE WHEN quantity >= 198 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    quantity_tier,
    sports_rows,
    avg_quantity
FROM stats
ORDER BY avg_quantity DESC NULLS LAST
