WITH cleaned AS (
    SELECT *, CONCAT(method, ' - ', status) AS method_status
    FROM payments
    WHERE paid_at > TO_DATE('2023-08-07')
      AND order_id = 31
)
SELECT id, order_id
FROM cleaned
EXCEPT
SELECT id, order_id
FROM payments
WHERE amount <= 880
