WITH store_totals AS (
    SELECT store_id, MIN(sale_date) AS first_sale_date
    FROM store_sales
    GROUP BY store_id
)
SELECT stores.id, stores.sqft, store_totals.first_sale_date
FROM stores
LEFT JOIN store_totals ON stores.id = store_totals.store_id
ORDER BY store_totals.first_sale_date DESC, stores.sqft;
