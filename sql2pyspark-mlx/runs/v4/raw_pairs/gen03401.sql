WITH filtered AS (
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        status,
        DATEDIFF(shipped_date, delivered_date) days_delivered_date_to_shipped_date,
        COALESCE(carrier, 'OTHER') resolved_carrier
    FROM shipments
    WHERE delivered_date >= '2025-04-01'
),
metrics AS (
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        status,
        days_delivered_date_to_shipped_date,
        resolved_carrier,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) cost_share,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id, status), 4) cost_share_2
    FROM filtered
),
filtered_shipments AS (
    SELECT *, ROUND(cost * 0.9, 2) cost_scaled
    FROM metrics
    WHERE delivered_date > DATE '2023-11-08'
),
with_windows AS (
    SELECT
        *,
        AVG(cost_share) OVER (PARTITION BY order_id, status ORDER BY delivered_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) cost_share_avg_6_prior,
        RANK() OVER (PARTITION BY status ORDER BY days_delivered_date_to_shipped_date DESC NULLS LAST) days_delivered_date_to_shipped_date_rank,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC, id) row_seq,
        CUME_DIST() OVER (PARTITION BY status ORDER BY cost_scaled DESC NULLS FIRST) cost_scaled_cume_dist
    FROM filtered_shipments
)
SELECT
    id,
    order_id,
    cost,
    status,
    days_delivered_date_to_shipped_date,
    resolved_carrier,
    cost_share,
    cost_share_2,
    cost_scaled,
    CASE WHEN status RLIKE '^(LOST|IN_TRANSIT)$' THEN 'CORE' WHEN status RLIKE '^(DELIVERED|RETURNED)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS status_zone
FROM with_windows
