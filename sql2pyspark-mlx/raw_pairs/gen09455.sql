WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS(',', channel, status) channel_status_key
    FROM orders
    WHERE order_date >= DATE '2023-04-22'
      AND customer_id = 22
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel, customer_id ORDER BY shipping_fee DESC, id) = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC, id) = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM deduped s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.order_date >= pr.starts_on
       AND s.order_date < pr.ends_on + INTERVAL '14' DAY
)
SELECT
    id,
    customer_id,
    store_id,
    order_date,
    status,
    channel,
    amount,
    promo_id,
    CASE WHEN channel RLIKE '^(partner|store)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS channel_zone
FROM matched
ORDER BY customer_id
