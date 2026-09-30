WITH filtered AS (
    SELECT *, ROUND(cost, 2) cost_rounded
    FROM shipments
    WHERE cost >= 31
      AND delivered_date > DATE '2023-09-20'
)
SELECT
    id,
    order_id,
    cost,
    status,
    LEAD(cost) OVER (PARTITION BY carrier ORDER BY cost_rounded, id) next_cost,
    LEAD(cost_rounded) OVER (PARTITION BY carrier ORDER BY cost_rounded, id) next_cost_rounded,
    ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost_rounded, id) row_seq
FROM filtered
WHERE delivered_date BETWEEN '2024-01-07' AND '2025-08-01';
