WITH StoreTotals AS (
    SELECT store_id, MAX(sale_date) AS latest_sale_date
    FROM store_sales
    WHERE store_id = 6
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    s.sqft,
    S.latest_sale_date
FROM stores s
INNER JOIN StoreTotals S ON s.id = S.store_id;
