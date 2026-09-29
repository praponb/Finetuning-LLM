WITH with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.quantity
    FROM orders s
    JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC NULLS LAST, id) AS latest_rank
        FROM with_latest
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
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
        quantity,
        latest_rank,
        CUME_DIST() OVER w_ordered AS amount_cume_dist,
        STDDEV_SAMP(amount) OVER (PARTITION BY customer_id, store_id) AS customer_id_stddev_samp_amount,
        NTILE(4) OVER (ORDER BY amount DESC, id) AS amount_bucket
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY channel ORDER BY amount DESC)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT status, id
    FROM enriched
    INTERSECT ALL
    SELECT status, id
    FROM orders
    WHERE discount <= 0.36
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.status = k.status AND s.id = k.id
),
with_windows AS (
    -- Running and moving aggregates
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
        quantity,
        latest_rank,
        amount_cume_dist,
        customer_id_stddev_samp_amount,
        amount_bucket,
        ROUND(customer_id_stddev_samp_amount / SUM(customer_id_stddev_samp_amount) OVER (PARTITION BY channel, status), 4) AS customer_id_stddev_samp_amount_share,
        DENSE_RANK() OVER (ORDER BY discount DESC) AS discount_dense_rank
    FROM reconciled
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.order_id IS NULL
        )
),
totals AS (
    -- Grouped metrics
    SELECT
        store_id,
        status,
        CASE WHEN discount_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS discount_dense_rank_tier,
        ROUND(SUM(discount_dense_rank) / NULLIF(COUNT(CASE WHEN status = 'RETURNED' THEN 1 END), 0), 2) AS discount_dense_rank_per_returned,
        ROUND(AVG(quantity), 2) AS avg_quantity,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        ROUND(SUM(discount_dense_rank) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS discount_dense_rank_per_mobile,
        SUM(discount) FILTER (WHERE channel = 'web') AS web_total_discount,
        SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
    FROM not_flagged
    GROUP BY GROUPING SETS ((store_id, status, CASE WHEN discount_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END), (store_id, status), (store_id))
)
SELECT
    status,
    discount_dense_rank_tier,
    channel_list,
    CASE WHEN status RLIKE '^(COMPLETED|SHIPPED)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS status_zone
FROM totals
