WITH joined AS (
    -- Attach orders attributes
    SELECT s.*, b.channel
    FROM shipments s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(cost) OVER (PARTITION BY status ORDER BY shipped_date, id) AS prev_cost,
        LEAD(cost) OVER (PARTITION BY status ORDER BY shipped_date, id) AS next_cost,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date, id) AS seq_no,
        LAST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost
    FROM joined
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(cost, 0) AS cost_filled,
        ADD_MONTHS(shipped_date, 12) AS shipped_date_plus_months
    FROM scored
    WHERE delivered_date > TO_DATE('2023-10-04')
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.status,
        src.next_cost,
        src.last_known_cost,
        src.cost_filled,
        src.shipped_date_plus_months,
        d.amount,
        d.order_date,
        d.status AS order_status
    FROM base src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_status, order_id ORDER BY amount, id) = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN next_cost >= 93 THEN 'High' WHEN next_cost >= 48 THEN 'Medium' ELSE 'Low' END AS next_cost_band,
        DATEDIFF(order_date, shipped_date_plus_months) / 7 AS weeks_shipped_date_plus_months_to_order_date
    FROM deduped
    WHERE order_id IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.status AS order_status_2,
        o.order_date AS order_order_date
    FROM cleaned src
    LEFT JOIN orders o
        ON src.order_id <=> o.id
)
SELECT
    id,
    order_id,
    carrier,
    status,
    next_cost,
    last_known_cost,
    cost_filled,
    amount,
    order_date,
    CASE WHEN carrier RLIKE '^(Kerry|FedEx)$' THEN 'GROUP_B' WHEN carrier RLIKE '^(DHL|Flash)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS carrier_zone
FROM enriched
ORDER BY order_id DESC
