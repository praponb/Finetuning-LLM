WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.shipping_fee,
        p.channel,
        p.discount
    FROM shipments s
    LEFT JOIN orders p
        ON s.order_id <=> p.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), shipped_date) / 30 AS months_since_shipped_date
    FROM with_dims
    WHERE channel IN ('web', 'partner', 'mobile')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        shipping_fee,
        discount,
        months_since_shipped_date,
        INITCAP(TRIM(carrier)) AS carrier_title,
        cost / NULLIF(discount, 0) AS cost_per_discount
    FROM valid
    WHERE channel = 'store'
),
with_dims_shipments AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.delivered_date,
        a.cost,
        a.discount,
        a.months_since_shipped_date,
        a.carrier_title,
        a.cost_per_discount,
        p.amount,
        p.status,
        p.channel
    FROM base a
    LEFT JOIN orders p
        ON a.order_id = p.id
    WHERE p.amount > 590
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_dims_shipments e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.delivered_date >= pr.starts_on
       AND e.delivered_date < pr.ends_on + INTERVAL 14 DAYS
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        cost,
        discount,
        months_since_shipped_date,
        cost_per_discount,
        amount,
        status,
        channel,
        promo_id,
        discount_pct,
        LEAD(cost, 1) OVER (PARTITION BY order_id ORDER BY delivered_date ASC, id ASC, promo_id ASC) AS next_cost,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY order_id), 4) AS discount_pct_share
    FROM with_window
),
scored_shipments AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        cost,
        months_since_shipped_date,
        status,
        channel,
        promo_id,
        discount_pct,
        discount_pct_share,
        SUM(cost) OVER (PARTITION BY order_id ORDER BY months_since_shipped_date ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        ROUND(next_cost / SUM(next_cost) OVER (PARTITION BY order_id), 4) AS next_cost_share,
        LAG(promo_id, 1) OVER (ORDER BY amount ASC, id ASC, promo_id ASC) AS prev_promo_id
    FROM scored
)
SELECT
    s.id,
    s.order_id,
    s.cost,
    s.status,
    s.promo_id,
    s.discount_pct_share,
    s.running_cost,
    s.next_cost_share,
    CASE WHEN status RLIKE '^(RETURNED|SHIPPED)$' THEN 'PRIORITY' WHEN status RLIKE '^(COMPLETED|PENDING)$' THEN 'EXTENDED' ELSE 'CORE' END AS status_zone,
    (SELECT MIN(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS min_shipment_cost
FROM scored_shipments s
ORDER BY min_shipment_cost ASC NULLS FIRST;
