WITH ProductSummary AS (
    SELECT product_id, MAX(sale_date) AS latest_sale_date
    FROM store_sales
    GROUP BY product_id
)
SELECT p.id, p.cost, P.latest_sale_date
FROM products p
INNER JOIN ProductSummary P ON p.id = P.product_id;
