WITH scored AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY city ORDER BY sqft, id) AS sqft_bucket,
        NTILE(3) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_bucket_2,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS event_seq
    FROM stores
),
latest_match AS (
    SELECT s.*, top1.discount
    FROM scored s
    JOIN LATERAL (
            SELECT c.discount
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.opened_date
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date >= s.opened_date
        )
),
latest_match_stores AS (
    SELECT s.*, x.order_shipping_fee
    FROM kept s
    JOIN LATERAL (
            SELECT c.shipping_fee AS order_shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    store_name,
    city,
    region,
    sqft_bucket,
    event_seq,
    discount,
    order_shipping_fee
FROM latest_match_stores
ORDER BY id DESC NULLS FIRST, sqft_bucket
