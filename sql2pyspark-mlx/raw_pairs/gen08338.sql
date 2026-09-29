WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.status AS order_status,
        d.shipping_fee,
        d.amount
    FROM shipments a
    LEFT JOIN orders d
        ON a.order_id <=> d.id
    WHERE d.discount < 0.34
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY carrier ORDER BY shipping_fee NULLS LAST) AS latest_rank
    FROM with_dims
    QUALIFY latest_rank = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.*,
        d.order_date,
        d.status AS order_status_2
    FROM deduped s
    JOIN orders d
        ON s.order_id <=> d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(amount) OVER w_grp AS next_amount,
        LAG(amount) OVER w_grp AS prev_amount,
        LEAD(shipping_fee) OVER w_grp AS next_shipping_fee,
        LAG(shipping_fee) OVER w_grp AS prev_shipping_fee,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM joined
    WINDOW w_grp AS (PARTITION BY status, order_status_2 ORDER BY latest_rank, id)
),
with_dims_shipments AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.status AS order_status_3,
        d.shipping_fee AS order_shipping_fee
    FROM with_windows e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM with_dims_shipments
    EXCEPT
    SELECT status, id
    FROM shipments
    WHERE shipped_date BETWEEN '2023-03-01' AND '2025-05-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims_shipments s
    JOIN kept_keys k ON s.status = k.status AND s.id = k.id
)
SELECT
    s.order_id,
    s.carrier,
    s.shipped_date,
    s.delivered_date,
    s.cost,
    s.order_status,
    s.latest_rank,
    s.order_date,
    s.order_status_2,
    CASE WHEN order_status RLIKE '^(CANCELLED|RETURNED)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS order_status_zone,
    (SELECT MIN(t2.quantity) FROM order_items t2 WHERE t2.order_id = s.order_id) AS min_order_item_quantity
FROM kept_rows s
ORDER BY carrier DESC NULLS FIRST, shipped_date DESC;
