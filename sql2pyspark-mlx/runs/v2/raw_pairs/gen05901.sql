WITH store_totals AS (
    SELECT store_id, MIN(sale_date) first_sale_date
    FROM store_sales
    GROUP BY store_id
)
SELECT s.id, s.region, st.first_sale_date
FROM stores s
INNER JOIN store_totals st ON s.id = st.store_id
ORDER BY id DESC
LIMIT 10
