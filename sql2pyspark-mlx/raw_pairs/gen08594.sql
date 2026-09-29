WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END AS is_kerry
    FROM shipments
    WHERE delivered_date >= DATE '2023-05-29'
      AND order_id IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT order_id
    FROM valid
    EXCEPT
    SELECT order_id
    FROM shipments
    WHERE order_id IN (10, 40)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(shipped_date, delivered_date) / 7 AS weeks_delivered_date_to_shipped_date
    FROM reconciled
    WHERE delivered_date >= DATE '2023-06-03'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(weeks_delivered_date_to_shipped_date) OVER (PARTITION BY order_id) AS order_id_avg_weeks_delivered_date_to_shipped_date,
        MAX(is_kerry) OVER (PARTITION BY order_id, status) AS order_id_max_is_kerry,
        ROUND(cost / SUM(cost) OVER (PARTITION BY DATE_TRUNC('WEEK', delivered_date), status), 4) AS cost_share,
        AVG(is_kerry) OVER (PARTITION BY order_id) AS order_id_avg_is_kerry
    FROM base
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        status,
        is_kerry,
        weeks_delivered_date_to_shipped_date,
        order_id_max_is_kerry,
        cost_share,
        weeks_delivered_date_to_shipped_date + cost_share AS weeks_delivered_date_to_shipped_date_plus_cost_share
    FROM with_windows
    WHERE order_id_avg_is_kerry BETWEEN 28 AND 59
)
-- Join a dimension and aggregate
SELECT
    o.status AS order_status,
    s.status AS shipment_status,
    COUNT(*) AS cnt,
    ROUND(SUM(s.is_kerry), 2) AS total_is_kerry
FROM cleaned s
LEFT JOIN orders o ON s.order_id = o.id
GROUP BY ROLLUP (o.status, s.status)
ORDER BY order_status;
