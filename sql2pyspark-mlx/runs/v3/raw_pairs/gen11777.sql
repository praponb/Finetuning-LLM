SELECT
    cost,
    status,
    delivered_date,
    DATE_FORMAT(shipped_date, 'MMM yyyy') AS shipped_date_label,
    DATE_FORMAT(shipped_date, 'yyyy-MM-dd') AS shipped_date_label_2
FROM shipments
WHERE (cost BETWEEN 30 AND 91 AND delivered_date BETWEEN '2023-06-01' AND '2024-08-08') OR cost >= 85
ORDER BY shipped_date_label_2, cost DESC
