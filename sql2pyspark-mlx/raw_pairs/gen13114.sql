WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(discount) OVER (PARTITION BY DATE_TRUNC('YEAR', order_date), customer_id ORDER BY order_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount,
        ROW_NUMBER() OVER (ORDER BY discount ASC, id ASC) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY amount ASC, id ASC) AS event_seq
    FROM orders
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.order_id IN (9, 12, 24)
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(event_seq) OVER w_grp AS next_event_seq,
        LEAD(seq_no) OVER w_grp AS next_seq_no,
        ROW_NUMBER() OVER w_grp AS seq_no_2
    FROM eligible
    WHERE seq_no IS NULL
    WINDOW w_grp AS (PARTITION BY status, store_id ORDER BY seq_no ASC, id ASC)
),
scored_orders AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(running_discount) OVER (PARTITION BY store_id ORDER BY next_event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_discount,
        COUNT(running_discount) OVER (PARTITION BY store_id ORDER BY next_event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_discount,
        SUM(running_discount) OVER (PARTITION BY store_id ORDER BY next_event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_discount,
        LEAD(discount, 1) OVER (PARTITION BY channel ORDER BY seq_no_2 ASC, id ASC) AS next_discount
    FROM enriched
),
scored_orders_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        discount,
        shipping_fee,
        seq_no,
        next_seq_no,
        running_max_running_discount,
        next_discount,
        CUME_DIST() OVER w_recent AS next_seq_no_cume_dist,
        MAX(running_max_running_discount) OVER (PARTITION BY MONTH(order_date), channel) AS order_date_month_max_running_max_running_discount
    FROM scored_orders
    WINDOW w_recent AS (PARTITION BY YEAR(order_date), channel ORDER BY next_seq_no ASC NULLS LAST)
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.method,
        top1.paid_at
    FROM scored_orders_2 s
    JOIN LATERAL (
            SELECT c.method, c.paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_latest
    EXCEPT ALL
    SELECT id
    FROM orders
    WHERE channel IN ('mobile', 'partner')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    shipping_fee,
    seq_no,
    next_seq_no,
    running_max_running_discount,
    next_discount,
    next_seq_no_cume_dist,
    paid_at
FROM reconciled;
