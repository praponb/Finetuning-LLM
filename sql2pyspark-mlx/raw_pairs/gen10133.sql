WITH enriched AS (
    -- Attach stores attributes
    SELECT
        s.*,
        d.sqft,
        d.opened_date
    FROM orders s
    LEFT JOIN stores d
        ON s.store_id = d.id
    WHERE d.store_name IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(shipping_fee) OVER (PARTITION BY customer_id, store_id ORDER BY opened_date, id) AS prev_shipping_fee,
        LEAD(discount) OVER (PARTITION BY customer_id, store_id ORDER BY opened_date, id) AS next_discount,
        LAG(discount) OVER (PARTITION BY customer_id, store_id ORDER BY opened_date, id) AS prev_discount,
        ROW_NUMBER() OVER (PARTITION BY customer_id, store_id ORDER BY opened_date, id) AS row_seq
    FROM enriched
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        discount - row_seq AS discount_minus_row_seq,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM scored
    WHERE shipping_fee >= 17
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        sqft,
        opened_date,
        prev_shipping_fee,
        next_discount,
        prev_discount,
        row_seq,
        discount_minus_row_seq,
        sqft * prev_discount AS sqft_times_prev_discount,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM valid
    WHERE channel IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date >= s.order_date
        )
)
-- Window metrics over several different partitions
SELECT
    *,
    LAG(discount_minus_row_seq, 2, 0) OVER (PARTITION BY store_id ORDER BY sqft_times_prev_discount, id) AS prev_discount_minus_row_seq,
    LAG(next_discount, 1, 0) OVER (PARTITION BY channel ORDER BY opened_date, id) AS prev_next_discount
FROM not_flagged
