WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        COALESCE(discount, 0) AS discount_filled
    FROM orders
    WHERE order_date >= DATE '2023-08-04'
      AND customer_id IS NOT NULL
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM base s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.order_date >= pr.starts_on
       AND s.order_date <= DATE_ADD(pr.ends_on, 7)
),
attributed_orders AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM attributed src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date < pr.ends_on + INTERVAL '7' DAY
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(promotion_discount_pct AS INT) AS promotion_discount_pct_int,
        CEIL(shipping_fee) AS shipping_fee_whole,
        COALESCE(promo_code, 'DIRECT') AS promo_code_filled
    FROM attributed_orders
    WHERE order_date > DATE '2023-03-16'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        amount,
        discount_filled,
        promo_id,
        promo_code,
        promo_id_2,
        promotion_discount_pct,
        promo_code_filled
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, promotion_promo_code ORDER BY promo_id_2 DESC NULLS LAST, id ASC, promo_id ASC, promo_id_2 ASC) = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM ranked
    INTERSECT
    SELECT id
    FROM orders
    WHERE status NOT IN ('COMPLETED', 'CANCELLED', 'PENDING')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount,
        ROUND(promotion_discount_pct / SUM(promotion_discount_pct) OVER (PARTITION BY promo_code), 4) AS promotion_discount_pct_share,
        ROUND(amount / SUM(amount) OVER (PARTITION BY promo_code), 4) AS amount_share
    FROM verified
)
SELECT amount, amount_share
FROM enriched
ORDER BY amount_share ASC;
