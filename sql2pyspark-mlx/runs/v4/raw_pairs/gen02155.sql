SELECT stores.city, CASE WHEN YEAR(stores.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(stores.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort, SUM(CASE WHEN shipments.status = 'LOST' THEN 1 ELSE 0 END) AS lost_count
FROM shipments
INNER JOIN orders ON orders.id = shipments.order_id
RIGHT OUTER JOIN stores ON orders.store_id = stores.id
WHERE stores.opened_date <= '2025-03-01'
GROUP BY stores.city, CASE WHEN YEAR(stores.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(stores.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY 3
