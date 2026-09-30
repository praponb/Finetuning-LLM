WITH cleaned AS (
    SELECT *, DATE_SUB(sale_date, 41) AS sale_date_minus_41d
    FROM store_sales
    WHERE units = 31
)
SELECT id, product_id
FROM cleaned
EXCEPT
SELECT id, product_id
FROM store_sales
WHERE sale_date < '2025-10-01';
