WITH not_flagged AS (
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock >= s.opened_date
        )
),
latest_match AS (
    SELECT
        s.*,
        x.store_sale_units,
        x.sale_date
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units, c.sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match_stores AS (
    SELECT s.*, top1.order_order_date
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.order_date AS order_order_date
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
        ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY order_order_date, id) AS seq_no
    FROM latest_match_stores
)
SELECT *
FROM (
    SELECT
        region,
        city,
        seq_no
    FROM enriched
) src
PIVOT (
    AVG(seq_no) AS total
    FOR city IN ('New York' AS new_york, 'Bangkok' AS bangkok, 'London' AS london)
)
