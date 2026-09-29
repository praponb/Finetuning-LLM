WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM latest s
    JOIN promotions pr
        ON s.channel = pr.channel
       AND s.order_date >= pr.starts_on
       AND s.order_date < pr.ends_on + INTERVAL 14 DAYS
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM matched
    INTERSECT
    SELECT id, customer_id
    FROM orders
    WHERE status = 'COMPLETED'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM matched s
    JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        promo_id,
        promo_code,
        discount_pct,
        LAST_DAY(order_date) order_date_month_end,
        COALESCE(channel, 'UNKNOWN') channel_filled
    FROM reconciled
    WHERE order_date >= DATE '2023-06-24'
      AND channel IS DISTINCT FROM 'partner'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(promo_code) promo_code_lower,
        discount * amount discount_times_amount
    FROM valid
    WHERE customer_id IS NOT NULL
      AND amount BETWEEN 670 AND 750
)
SELECT
    s.id,
    s.customer_id,
    s.discount,
    s.shipping_fee,
    s.promo_id,
    s.promo_code,
    s.order_date_month_end,
    s.channel_filled,
    s.promo_code_lower,
    (SELECT AVG(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS avg_account_balance
FROM base s
