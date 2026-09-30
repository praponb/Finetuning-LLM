WITH latest_match AS (
    SELECT
        s.id,
        s.city,
        s.opened_date,
        latest.store_sale_sale_date,
        latest.revenue
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date <= s.opened_date
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match_stores AS (
    SELECT
        s.id,
        s.opened_date,
        s.store_sale_sale_date,
        s.revenue,
        latest.quantity
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    SELECT s.*
    FROM latest_match_stores s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date < s.store_sale_sale_date
        )
),
kept AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date > '2023-07-01'
        )
),
kept_stores AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.status != 'RETURNED'
        )
),
not_flagged AS (
    SELECT s.*
    FROM kept_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date <= s.store_sale_sale_date
        )
)
SELECT
    id,
    opened_date,
    ROW_NUMBER() OVER (ORDER BY opened_date DESC, id ASC) AS event_seq,
    NTILE(4) OVER w_recent AS revenue_bucket,
    LAST_VALUE(revenue, TRUE) OVER (ORDER BY store_sale_sale_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue
FROM not_flagged
WHERE MONTH(store_sale_sale_date) IN (5, 9, 10)
WINDOW w_recent AS (PARTITION BY DATE_TRUNC('week', opened_date) ORDER BY revenue DESC, id ASC);
