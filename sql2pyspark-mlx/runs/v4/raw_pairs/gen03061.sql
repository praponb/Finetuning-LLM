WITH kept_keys AS (
    -- Set difference on keys
    SELECT status
    FROM shipments
    EXCEPT DISTINCT
    SELECT status
    FROM shipments
    WHERE carrier = 'DHL'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM shipments s
    INNER JOIN kept_keys k ON s.status = k.status
),
joined AS (
    -- Attach orders attributes
    SELECT src.*, p.channel
    FROM reconciled src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY cost DESC) AS cost_rank,
        COUNT(cost) OVER (PARTITION BY status) AS status_count_cost,
        CUME_DIST() OVER (ORDER BY cost DESC) AS cost_cume_dist
    FROM joined
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(status_count_cost / cost, 2) AS status_count_cost_per_cost,
        DATE_ADD(delivered_date, 60) AS delivered_date_plus_60d
    FROM scored
    WHERE delivered_date >= TO_DATE('2023-06-18')
),
grouped AS (
    -- Grouped metrics
    SELECT
        order_id,
        AVG(status_count_cost_per_cost) FILTER (WHERE carrier = 'FedEx') AS fedex_avg_status_count_cost_per_cost,
        ROUND(SUM(status_count_cost_per_cost) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS status_count_cost_per_cost_per_mobile,
        MAX(status_count_cost) FILTER (WHERE carrier = 'UPS') AS ups_max_status_count_cost
    FROM base
    GROUP BY 1
    HAVING MAX(status_count_cost) FILTER (WHERE carrier = 'UPS') >= 0
)
SELECT
    s.order_id,
    s.status_count_cost_per_cost_per_mobile,
    s.ups_max_status_count_cost,
    (SELECT SUM(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS sum_shipment_cost
FROM grouped s
