SELECT
    YEAR(shipped_date) shipped_year,
    order_id,
    MIN(cost) min_cost,
    ROUND(AVG(cost), 2) avg_cost,
    ROUND((100 * SUM(CASE WHEN carrier = 'DHL' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_dhl
FROM shipments
WHERE cost IS NULL AND delivered_date BETWEEN '2024-08-01' AND '2024-12-21'
GROUP BY YEAR(shipped_date), order_id
HAVING SUM(cost) > 460
ORDER BY pct_dhl DESC, shipped_year, order_id
LIMIT 3;
