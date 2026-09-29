SELECT delivered_date, cost, COALESCE(cost, 0) AS cost_filled FROM shipments WHERE carrier = 'Flash' AND YEAR(shipped_date) = 2025
