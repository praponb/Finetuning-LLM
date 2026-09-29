WITH joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.shipping_fee,
        b.order_date
    FROM shipments e
    LEFT JOIN orders b
        ON e.order_id = b.id
    WHERE b.store_id IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_SUB(shipped_date, 89) AS shipped_date_minus_89d,
        ADD_MONTHS(order_date, 6) AS order_date_plus_months
    FROM joined
    WHERE shipped_date > DATE '2023-09-03'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT status, carrier
    FROM base
    INTERSECT ALL
    SELECT status, carrier
    FROM shipments
    WHERE delivered_date <= '2024-06-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN matched_keys k ON s.status = k.status AND s.carrier = k.carrier
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        shipping_fee * cost AS shipping_fee_times_cost,
        DATE_FORMAT(order_date_plus_months, 'yyyy-MM-dd') AS order_date_plus_months_label
    FROM verified
    WHERE carrier IS NOT NULL
      AND carrier IN ('DHL', 'FedEx', 'Flash')
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.cost,
    s.status,
    s.shipped_date_minus_89d,
    s.order_date_plus_months,
    s.shipping_fee_times_cost,
    CASE WHEN status RLIKE '^(DELIVERED|LOST)$' THEN 'GROUP_A' WHEN status RLIKE '^(IN_TRANSIT|RETURNED)$' THEN 'CORE' ELSE 'PRIORITY' END AS status_zone,
    (SELECT SUM(t2.quantity) FROM order_items t2 WHERE t2.order_id = s.order_id) AS sum_order_item_quantity
FROM filtered s
ORDER BY cost ASC, sum_order_item_quantity DESC;
