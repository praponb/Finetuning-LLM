SELECT status, order_quarter, pending_count
FROM (
    SELECT status, QUARTER(order_date) AS order_quarter, SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count
    FROM orders
    WHERE order_date BETWEEN '2023-03-01' AND '2023-07-09'
    GROUP BY status, QUARTER(order_date)
) AS sub
WHERE pending_count >= 49;
