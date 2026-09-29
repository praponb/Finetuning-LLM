SELECT
    id,
    shipped_date,
    cost,
    COALESCE(cost, 0) cost_filled,
    MONTH(delivered_date) delivered_date_month
FROM shipments
WHERE status NOT IN ('DELIVERED', 'IN_TRANSIT', 'RETURNED')
