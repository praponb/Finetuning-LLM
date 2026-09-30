WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(paid_at, 'yyyy-MM-dd') AS paid_at_label,
        TRIM(method) AS clean_method
    FROM payments
    WHERE paid_at >= TO_DATE('2023-09-03')
      AND order_id IS NOT NULL
)
SELECT method, status
FROM valid
EXCEPT
SELECT method, status
FROM payments
WHERE order_id IN (17, 29, 39);
