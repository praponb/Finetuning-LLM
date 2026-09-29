SELECT *, DATE_SUB(shipped_date, 12) AS shipped_date_minus_12d
FROM shipments
WHERE (delivered_date <= '2025-10-29' AND shipped_date < '2024-08-01') OR delivered_date BETWEEN '2023-03-28' AND '2023-12-01'
ORDER BY order_id DESC, id
LIMIT 3;
