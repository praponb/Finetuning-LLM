SELECT
    id,
    carrier,
    shipped_date,
    RANK() OVER (PARTITION BY carrier ORDER BY shipped_date) AS shipped_date_rank
FROM shipments
WHERE shipped_date BETWEEN '2023-04-27' AND '2025-05-01';
