WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), order_date) days_since_order_date,
        COALESCE(channel, 'NONE') channel_clean
    FROM orders
    WHERE store_id IS NOT NULL
      AND status = 'CANCELLED'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(shipping_fee) OVER w_recent prev_shipping_fee,
        LEAD(days_since_order_date) OVER w_recent next_days_since_order_date,
        LAG(days_since_order_date) OVER w_recent prev_days_since_order_date,
        ROW_NUMBER() OVER w_recent row_seq
    FROM filtered
    WINDOW w_recent AS (PARTITION BY store_id ORDER BY shipping_fee, id)
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_id,
        s.status,
        s.channel,
        s.amount,
        s.discount,
        s.days_since_order_date,
        s.channel_clean,
        s.prev_shipping_fee,
        s.row_seq,
        top1.payment_status,
        top1.payment_method
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.status AS payment_status, c.method AS payment_method
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.paid_at >= '2025-02-01'
        )
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.product_id IN (6, 28, 31, 40)
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        status,
        amount,
        days_since_order_date,
        channel_clean,
        row_seq
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, payment_method ORDER BY days_since_order_date DESC, id) AS latest_rank
        FROM kept
    ) ranked
    WHERE latest_rank = 1
),
eligible_orders AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.product_id IS NOT NULL
        )
)
-- Grouped metrics
SELECT
    status,
    CASE WHEN row_seq >= 9 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    PERCENTILE_APPROX(days_since_order_date, ARRAY(0.25, 0.5, 0.75)) AS days_since_order_date_quartiles,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count
FROM eligible_orders
GROUP BY 1, 2
