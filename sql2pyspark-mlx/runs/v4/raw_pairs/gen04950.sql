WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        CASE WHEN channel IN ('partner', 'web') THEN 'Core' ELSE 'Other' END channel_group,
        COALESCE(channel, 'OTHER') resolved_channel
    FROM orders
    WHERE order_date > DATE '2023-05-14'
      AND channel IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        status,
        amount,
        discount,
        shipping_fee,
        resolved_channel,
        LAST_DAY(order_date) order_date_month_end,
        amount / NULLIF(discount, 0) amount_per_discount
    FROM base
    WHERE status NOT IN ('COMPLETED', 'PENDING', 'RETURNED')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        status,
        amount,
        discount,
        shipping_fee,
        resolved_channel,
        order_date_month_end,
        amount_per_discount,
        CORR(amount, discount) OVER (PARTITION BY store_id) corr_amount_discount,
        LAG(amount_per_discount, 1) OVER (PARTITION BY status ORDER BY order_date_month_end, id) prev_amount_per_discount
    FROM valid
),
matched_keys AS (
    -- Set difference on keys
    SELECT status
    FROM metrics
    EXCEPT ALL
    SELECT status
    FROM orders
    WHERE channel = 'mobile'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.status = k.status
),
joined AS (
    -- Attach stores attributes
    SELECT s.*, b.opened_date
    FROM reconciled s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(prev_amount_per_discount) OVER (PARTITION BY status) status_max_prev_amount_per_discount,
        RANK() OVER w_recent corr_amount_discount_rank,
        RANK() OVER (ORDER BY amount_per_discount NULLS LAST) amount_per_discount_rank,
        PERCENT_RANK() OVER w_running prev_amount_per_discount_percent_rank
    FROM joined
    WINDOW w_recent AS (ORDER BY corr_amount_discount NULLS LAST), w_running AS (PARTITION BY store_id ORDER BY prev_amount_per_discount DESC)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        store_id,
        shipping_fee,
        prev_amount_per_discount
    FROM enriched
) src
UNPIVOT (
    val FOR metric IN (shipping_fee, prev_amount_per_discount AS PREV_AMOUNT_PER_DISCOUNT)
);
